package workmgr

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const priorityLevels = 5

const tolerance = 1e-6

type schedulerConfig struct {
	// targetMultiple determines the multiplier used to calculate how many tokens are needed to maintain
	// steady back-pressure on the activeWorkQueue. The nextTokens() function uses this factor to
	// estimate the number of tokens required to sustain targetMultiple * averageComplete work. A smaller
	// targetMultiple can cause worker starvation during sudden increases in completed work, while a
	// larger targetMultiple may delay the execution of higher-priority requests.
	targetMultiple float64
	// alpha controls the weighting of previous values in the exponentially weighted moving averages
	// (averageDeltaTimeMs and averageCompletePerMs). The halfLifeSec parameter determines how quickly the
	// previous average decays. A larger halfLifeSec produces smoother but slower responses
	// to changes in workload, while a smaller halfLifeSec reacts more quickly but with greater volatility.
	halfLifeSec float64
	// maximumAllowedTokenGrowth limits the growth of the allowed tokens in a given cycle. Larger values
	// response faster to spikes in completed work whereas smaller values will be slow and more
	// controlled. Extremely large values can cause oscillations in growth.
	maximumAllowedTokenGrowth float64
	// minimumAllowedTokens is the minimum allowed tokens that can be distributed among the
	// priority queues per cycle. This value will be multipled by the targetMultiple to define a
	// starting number of tokens to grow from. This should at least be the number of workers.
	minimumAllowedTokens int
	// pullInWorkTicker defines the periodic release of work tokens.
	pullInWorkTicker time.Duration
}

type schedulerOpt func(*schedulerConfig)

func WithMinimumAllowedTokens(tokens int) schedulerOpt {
	return func(cfg *schedulerConfig) {
		cfg.minimumAllowedTokens = tokens
	}
}

type scheduler struct {
	ctx context.Context
	log *zap.Logger
	// Weights are used to distribute tokens fairly between the priority queues as to avoid starving
	// lower priorities while giving preference to higher priorities. These weights are dynamically
	// adjust to exclude empty queues.
	weights [priorityLevels]float64
	// targetMultiple determines the multiplier used to calculate how many tokens are needed to maintain
	// steady back-pressure on the activeWorkQueue. The nextTokens() function uses this factor to
	// estimate the number of tokens required to sustain targetMultiple * averageComplete work. A smaller
	// targetMultiple can cause worker starvation during sudden increases in completed work, while a
	// larger targetMultiple may delay the execution of higher-priority requests.
	targetMultiple float64
	// capacity is the largest number of work requests the active work queue channel can have.
	capacity int
	// nextPriority returns the next priority level in a rotating sequence, cycling through all
	// priorities once before returning false.
	nextPriority func() (int, bool)
	// workTokens maintains the number of alloted work workTokens for each priority. Work workTokens are
	// used by the manager's work scheduler in order to maintain fairness between priority queues
	// and back-pressure on the activeWorkQueue. The priority workTokens are used for both priority and
	// wait queues.
	workTokens [priorityLevels]atomic.Int32
	// minimumAllowedTokens is the minimum allowed tokens that can be distributed among the
	// priority queues per cycle.
	minimumTokensToDistribute int
	// maximumAllowedTokenGrowth limits the growth of the allowed tokens in a given cycle. Larger values
	// response faster to spikes in completed work whereas smaller values will be slow and more
	// controlled. Extremely large values can cause oscillations in growth.
	maximumAllowedTokenGrowth float64
	// alpha determines how much weight to give to past samples and is used in computing
	// averageDurationMs and averageCompletedWorkPerMs. It must between 0 and 1. Smaller values will
	// give more influence to past samples whereas larger values give the influence to the current.
	alpha                float64
	previousUsedCapacity int
	// previousTokensDistributed tracks the total number of tokens distributed.
	previousTokensDistributed int
	previousTime              time.Time
	averageDurationMs         float64
	averageCompletedWorkPerMs float64
	// lowWorkThreshold is the pending requests low threshold. If the active work queue requests
	// falls below this value then the shortSendWorkTicker will send more work.
	lowWorkThreshold int
	// lowWorkThresholdPct is the percentage of average work throughput that defines the low
	// threshold.
	lowWorkThresholdPct float64
	// allWorkTokens is a counter that maintains the total number of existing workTokens. This is
	// used to release more tokens when the active work queue drops below a certain threshold.
	allWorkTokens atomic.Int32
	// tokensReleased is a buffered channel releases the priority tokens to the sync manager. This
	// must be buffered to avoid deadlocking with manager. The channel must hold every sendWork that
	// can occur before the manager loop reads m.scheduler.tokensReleased.
	tokensReleased           chan [priorityLevels]int
	shouldReportIdleStatus   bool
	shouldReportActiveStatus bool
}

func NewScheduler(ctx context.Context, log *zap.Logger, queue chan workAssignment, fairness gemetricRatio, opts ...schedulerOpt) (s *scheduler, close func() error) {

	cfg := &schedulerConfig{
		targetMultiple:            2.5,
		halfLifeSec:               10,
		maximumAllowedTokenGrowth: 0.85,
		minimumAllowedTokens:      32,
		pullInWorkTicker:          1000 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	s = &scheduler{
		ctx:                       ctx,
		log:                       log,
		weights:                   geometricFairnessWeights(fairness),
		targetMultiple:            cfg.targetMultiple,
		minimumTokensToDistribute: int(cfg.targetMultiple * float64(cfg.minimumAllowedTokens)),
		maximumAllowedTokenGrowth: cfg.maximumAllowedTokenGrowth,
		capacity:                  cap(queue),
		nextPriority:              GetNextPriority(),
		alpha:                     1 - math.Exp(-math.Ln2/cfg.halfLifeSec),
		tokensReleased:            make(chan [priorityLevels]int, 1),
		shouldReportIdleStatus:    false,
		shouldReportActiveStatus:  true,
	}
	s.log.Info("worker node is idle")

	pullInWorkTicker := time.NewTicker(cfg.pullInWorkTicker)
	close = func() error {
		pullInWorkTicker.Stop()
		return nil
	}

	go func() {
		s.previousTime = time.Now()
		s.averageDurationMs = float64(cfg.pullInWorkTicker.Milliseconds())
		for {
			select {
			case <-s.ctx.Done():
				return
			case currentTime := <-pullInWorkTicker.C:
				usedCapacity := len(queue)

				elapsedTimeMs := float64(currentTime.Sub(s.previousTime).Milliseconds())
				s.averageDurationMs = s.alpha*elapsedTimeMs + (1-s.alpha)*s.averageDurationMs
				s.previousTime = currentTime

				completedWork := float64(s.previousUsedCapacity + s.previousTokensDistributed - usedCapacity)
				completedWorkPerMs := completedWork / elapsedTimeMs
				s.previousUsedCapacity = usedCapacity
				s.sendWork(usedCapacity, completedWorkPerMs)
			}
		}
	}()

	return
}

// AddWorkToken(submissionID) tells the scheduler about a submission in the journal that is eligible
// to be scheduled. It should be called whenever a WR is created, rediscovered on startup, or
// rescheduled. The scheduler decodes the submission ID to determine the priority and increment that
// bucket's token count. These counts are used to keep track of pending work at each priority to
// ensure no priority queue is starved. Tokens represent work that is ready but not yet dispatched
// to a worker.
func (s *scheduler) AddWorkToken(submissionId string) {
	priority := submissionIdPriority(submissionId)
	s.workTokens[priority].Add(1)
	s.allWorkTokens.Add(1)
}

// RemoveWorkToken(submissionID) is called once a work assignment has been added to the active work
// queue (not when it actually completes). This tells the scheduler a request at the given priority
// has been handed to a worker, allowing it to internally adjust how it assigns new work.
func (s *scheduler) RemoveWorkToken(submissionId string) {
	priority := submissionIdPriority(submissionId)
	s.workTokens[priority].Add(-1)
	s.allWorkTokens.Add(-1)
}

func (s *scheduler) sendWork(currentUsedCapacity int, completedWorkPerMs float64) {
	tokensAllowed := s.getTokensAllowed(currentUsedCapacity, completedWorkPerMs)
	tokens, isWork := s.distributeTokens(tokensAllowed)
	if isWork {
		s.tokensReleased <- tokens
		if s.shouldReportActiveStatus {
			s.log.Info("worker node is no longer idle")
			s.shouldReportIdleStatus = true
			s.shouldReportActiveStatus = false
		}
	} else if s.shouldReportIdleStatus && s.allWorkTokens.Load() == 0 {
		s.log.Info("worker node is idle")
		s.shouldReportIdleStatus = false
		s.shouldReportActiveStatus = true
	}
}

// Return the token allowed to be scheduled. The tokens determined by target multiple of
// completed work, adjusted by the recent growth trend. This calculation attempts to:
//   - Prevent worker starvation during short throughput bursts.
//   - Avoid saturating the activeWorkQueue so it can drain quickly enough to minimize priority
//     inversion (long delays for high-priority work caused by a backlog of lower-priority tasks).
func (s *scheduler) getTokensAllowed(usedCapacity int, completedWorkPerMs float64) (tokensAllowed int) {
	availableCapacity := s.capacity - usedCapacity

	if s.averageCompletedWorkPerMs <= tolerance {
		s.averageCompletedWorkPerMs = completedWorkPerMs
		tokensAllowed = s.minimumTokensToDistribute
		s.log.Debug("token scheduler warmup allowance",
			zap.Int("usedCapacity", usedCapacity),
			zap.Int("availableCapacity", availableCapacity),
			zap.Int("tokensAllowed", tokensAllowed),
		)
	} else {

		var growthFactor float64 = 0
		growthDenominator := math.Abs(s.averageCompletedWorkPerMs)
		if growthDenominator > tolerance {
			growthFactor = (completedWorkPerMs - s.averageCompletedWorkPerMs) / growthDenominator
			if growthFactor > s.maximumAllowedTokenGrowth {
				growthFactor = s.maximumAllowedTokenGrowth
			} else if growthFactor < -s.maximumAllowedTokenGrowth {
				growthFactor = -s.maximumAllowedTokenGrowth
			}
		}

		s.averageCompletedWorkPerMs = s.alpha*completedWorkPerMs + (1-s.alpha)*s.averageCompletedWorkPerMs
		if s.averageCompletedWorkPerMs < 1.0/s.averageDurationMs {
			s.averageCompletedWorkPerMs = 0
		}
		normalizedCompletedWork := s.averageCompletedWorkPerMs * s.averageDurationMs
		s.lowWorkThreshold = int(math.Ceil(normalizedCompletedWork * s.lowWorkThresholdPct))

		targetSlots := int(s.targetMultiple * (1 + growthFactor) * normalizedCompletedWork)
		tokensAllowed = max(targetSlots, s.minimumTokensToDistribute) - usedCapacity
		s.log.Debug("token scheduler computed allowance",
			zap.Int("usedCapacity", usedCapacity),
			zap.Int("availableCapacity", availableCapacity),
			zap.Float64("normalizedCompletedWork", RoundToMillis(normalizedCompletedWork)),
			zap.Float64("growth", RoundToMillis(growthFactor)),
			zap.Int("tokensAllowed", tokensAllowed),
		)
	}

	if tokensAllowed > availableCapacity {
		tokensAllowed = availableCapacity
	} else if tokensAllowed < 0 {
		tokensAllowed = 0
	}
	return tokensAllowed
}

func (s *scheduler) distributeTokens(tokensAllowed int) (tokens [priorityLevels]int, isWork bool) {
	if tokensAllowed <= 0 {
		return
	}

	// Distribute tokens among the priority queues. The normalizer is used to handle empty
	// priority queues by distributing their allocations to the queues with work. Any remainder
	// that does not evenly distribute to each priority queue will be distributed to queues with
	// the highest priorities first. Note that that alloted tokens (ie tokensLeft)
	normalizer := 1.0
	workTokens := [priorityLevels]int{}
	for priority := range priorityLevels {
		workTokens[priority] = int(s.workTokens[priority].Load())
		if workTokens[priority] <= 0 {
			normalizer -= s.weights[priority]
		}
	}
	if normalizer < tolerance {
		s.previousTokensDistributed = 0
		s.log.Debug("all queues are empty")
		return
	}

	isWork = true
	tokensLeft := tokensAllowed
	for priority, ok := s.nextPriority(); ok && tokensLeft > 0; priority, ok = s.nextPriority() {
		if workTokens[priority] <= 0 {
			continue
		}
		allowed := int(s.weights[priority] / normalizer * float64(tokensLeft))
		tokens[priority] = min(allowed, workTokens[priority])
		tokensLeft -= tokens[priority]
		workTokens[priority] -= tokens[priority]
	}

	for priority := range priorityLevels {
		if tokensLeft <= 0 || workTokens[priority] <= 0 {
			continue
		}
		if workTokens[priority] >= tokensLeft {
			tokens[priority] += tokensLeft
			tokensLeft = 0
		} else {
			tokens[priority] += workTokens[priority]
			tokensLeft -= workTokens[priority]
		}
	}

	tokensUnused := tokensLeft
	tokensDistributed := tokensAllowed - tokensLeft
	s.log.Debug("token scheduler distribution",
		zap.Int("tokensDistributed", tokensDistributed),
		zap.Int("tokensUnused", tokensUnused),
		zap.Any("tokensByPriority", tokens))
	s.previousTokensDistributed = tokensDistributed
	return
}

func RoundToMillis(x float64) float64 { return math.Round(x*1e3) / 1e3 }

/*
The highest base value for the submissionID is ^uint64(0) which is 3w5e11264sgsf in base-36. This
can be fit multiple times within 13-character base-36 string so defined ranges are utilized to
represent five priority ranges.

In order to simplify parsing, 8-9 have been ignore and priority 2 begins at the 'a'. The following
table shows the ranges. The lead-byte represents the first character in the submissionID string.

| ASCII | Offset | Lead Byte | Priority |
| ----- | ------ | --------- | -------- |
|   48  |    0   |    `0`    |    0     |
|   49  |    1   |    `1`    |    0     |
|   50  |    2   |    `2`    |    0     |
|   51  |    3   |    `3`    |    0     |
|   52  |    4   |    `4`    |    1     |
|   53  |    5   |    `5`    |    1     |
|   54  |    6   |    `6`    |    1     |
|   55  |    7   |    `7`    |    1     |
|   56  |    8   |    `8`    | ignored  |
|   57  |    9   |    `9`    | ignored  |
|   97  |   49   |    `a`    |    2     |
|   98  |   50   |    `b`    |    2     |
|   99  |   51   |    `c`    |    2     |
|  100  |   52   |    `d`    |    2     |
|  101  |   53   |    `e`    |    3     |
|  102  |   54   |    `f`    |    3     |
|  103  |   55   |    `g`    |    3     |
|  104  |   56   |    `h`    |    3     |
|  105  |   57   |    `i`    |    4     |
|  106  |   58   |    `j`    |    4     |
|  107  |   59   |    `k`    |    4     |
|  108  |   60   |    `l`    |    4     |
*/
const submissionIdPriorityTableStart = byte(48)

// submissionIdPriorityOffsetTable defines the submissionId boundaries for each priority.
// The offset range for a given priority spans from table[priority-1] to table[priority].
var submissionIdPriorityOffsetTable = []byte{0, 4, 49, 53, 57, 61}
var submissionIdPriorityStarts = []string{"0000000000000", "4000000000000", "a000000000000", "e000000000000", "i000000000000"}
var submissionIdPriorityStops = []string{"4000000000000", "a000000000000", "e000000000000", "i000000000000", "m000000000000"}

func SubmissionIdPriorityRange(priority int) (start, stop string) {
	return submissionIdPriorityStarts[priority], submissionIdPriorityStops[priority]
}

func CreateSubmissionId(baseKey string, workRequestPriority int32) (string, int32) {
	priority := workRequestPriority - 1
	if priority < 0 || priority > priorityLevels-1 {
		priority = 2
	}
	leadByte := baseKey[0] + submissionIdPriorityOffsetTable[priority]
	return string(leadByte) + baseKey[1:], priority + 1
}

func DemoteSubmissionId(key string) (string, int32) {
	baseKey := submissionBaseKey(key)
	workRequestPriority := min(submissionIdPriority(key)+1, priorityLevels-1) + 1
	return CreateSubmissionId(baseKey, workRequestPriority)
}

func PromoteSubmissionId(key string) (string, int32) {
	baseKey := submissionBaseKey(key)
	workRequestPriority := max(submissionIdPriority(key)-1, 0) + 1
	return CreateSubmissionId(baseKey, workRequestPriority)
}

func submissionIdPriority(key string) int32 {
	leadByte := key[0]
	i := int32(priorityLevels - 1)
	for ; i >= 0; i-- {
		priorityStartByte := submissionIdPriorityTableStart + submissionIdPriorityOffsetTable[i]
		if leadByte >= priorityStartByte {
			break
		}
	}
	return i
}

func submissionBaseKey(key string) string {
	leadByte := key[0]
	leadByte -= submissionIdPriorityOffsetTable[submissionIdPriority(key)]
	return string(leadByte) + key[1:]
}

// nextPriority returns the next priority level in a rotating sequence, cycling through all
// priorities once before returning false; after each full cycle, the starting priority shifts so
// every level eventually gets a turn to go first.
func GetNextPriority() func() (int, bool) {
	counter := 0
	id := 0
	return func() (int, bool) {
		id++
		if id == priorityLevels {
			id = 0
		}

		if counter == priorityLevels {
			counter = 0
			return -1, false
		}
		counter++
		return id, true
	}
}

type gemetricRatio float64

const (
	AGGRESSIVE gemetricRatio = 0.50  // [51.6 25.8 12.9 06.4 03.2]
	STRONG     gemetricRatio = 0.667 // [38.3 25.5 17.0 11.3 07.5]
	BALANCED   gemetricRatio = 0.75  // [32.7 24.5 18.4 13.8 10.3]
	GENTLE     gemetricRatio = 0.85  // [26.9 22.9 19.4 16.5 14.0]
	FAIR       gemetricRatio = 0.90  // [24.4 21.9 19.7 17.8 16.0]
)

func (r gemetricRatio) String() string {
	switch r {
	case AGGRESSIVE:
		return "aggressive"
	case STRONG:
		return "strong"
	case BALANCED:
		return "balanced"
	case GENTLE:
		return "gentle"
	case FAIR:
		return "fair"
	default:
		return strconv.FormatFloat(float64(r), 'f', 3, 64)
	}
}

// geometricFairnessWeights returns a normalized list of weights based on the geometricRatio.
func geometricFairnessWeights(ratio gemetricRatio) (weights [priorityLevels]float64) {
	normalizer := 0.0
	current := 1.0
	for range priorityLevels {
		normalizer += current
		current *= float64(ratio)
	}

	current = 1.0
	for i := range priorityLevels {
		weights[i] = current / normalizer
		current *= float64(ratio)
	}
	return weights
}
