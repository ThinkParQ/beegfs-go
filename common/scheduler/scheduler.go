package scheduler

import (
	"context"
	"expvar"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	priorityLevels = 5
	tolerance      = 1e-6
)

var (
	schedulerMetricsOnce          sync.Once
	schedulerCompletedWorkHz      *expvar.Float
	schedulerTokensAllowedHz      *expvar.Float
	schedulerPriorityFairnessMode *expvar.String
)

type schedulerConfig struct {
	// Name used to describe node in logging.
	nodeName string
	// Fairness ratio to generate priority weights.
	priorityFairness geometricRatio
	// allowedTokensMultiplier is the average allowed token multiplier used to calculate how many tokens
	// are needed to maintain steady back-pressure on the activeWorkQueue.
	//
	// Any value over one is the expected queue buffer between cycles which not only responds
	// immediately to influxes in work throughput but also contributes to the growth of the allowed
	// tokens per cycle. Ideally, it should provide just enough queue to get though a cycle without
	// excess so as not to starve the workers and yet give each priority near immediate access;
	// however, since the ideal doesn't account for spikes in work we must add a buffer.
	allowedTokensMultiplier float64
	// allowedTokensGrowthRateMax limits the growth of the allowed tokens in a given stat update cycle.
	// Larger values response faster to spikes in completed work whereas smaller values will be slow
	// and more controlled. Extremely large values can cause oscillations in growth.
	allowedTokensGrowthRateMax float64
	// allowedTokensMin is the minimum allowed tokens that can be distributed among the
	// priority queues per cycle. This value will be multiplied by the targetMultiple to define a
	// starting number of tokens to grow from. This should at least be the number of workers.
	allowedTokensMin int
	// averageWindow specifies the window of recent history to average. A large window result in
	// smoother and more gradual changes. Whereas a small window results in fast and abrupt changes.
	averageWindow time.Duration
	// releasesPerStatUpdate is the number of releaseTokensTicks before stats should be updated.
	releasesPerStatUpdate int
	// releaseTokensTick defines the periodic release of work tokens.
	releaseTokensTick time.Duration
}

type schedulerOpt func(*schedulerConfig)

func WithNodeName(name string) schedulerOpt {
	return func(cfg *schedulerConfig) {
		cfg.nodeName = name
	}
}

func WithFairness(ratio geometricRatio) schedulerOpt {
	return func(cfg *schedulerConfig) {
		cfg.priorityFairness = ratio
	}
}
func WithAllowedTokensBufferPct(percent float64) schedulerOpt {
	return func(cfg *schedulerConfig) {
		cfg.allowedTokensMultiplier = 1 + max(percent/100, 0)
	}
}

func WithAverageWindow(averageWindow time.Duration, updatesPerWindow int, releasesPerUpdate int) schedulerOpt {
	return func(cfg *schedulerConfig) {
		if updatesPerWindow < 1 {
			updatesPerWindow = 1
		}
		if releasesPerUpdate < 1 {
			releasesPerUpdate = 1
		}
		cfg.averageWindow = averageWindow
		cfg.releasesPerStatUpdate = releasesPerUpdate
		cfg.releaseTokensTick = averageWindow / time.Duration(updatesPerWindow*releasesPerUpdate)
	}
}

func WithAllowedTokensGrowthRateMax(rate float64) schedulerOpt {
	return func(cfg *schedulerConfig) {
		cfg.allowedTokensGrowthRateMax = rate
	}
}

func WithAllowedTokensMin(tokens int) schedulerOpt {
	return func(cfg *schedulerConfig) {
		cfg.allowedTokensMin = tokens
	}
}

type PriorityToken struct {
	Priority int
	Tokens   int
}

type Scheduler struct {
	ctx context.Context
	log *zap.Logger
	// workTokens maintains the number of alloted work workTokens for each priority. workTokens and
	// rescheduledWorkTokens are used by the manager's work scheduler in order to maintain fairness
	// between priority queues and back-pressure on the activeWorkQueue. The priority workTokens are
	// used for both priority and wait queues.
	workTokens [priorityLevels]atomic.Int32
	// rescheduledWork maintains the number of rescheduled work requests for each priority.
	rescheduledWork [priorityLevels]atomic.Int32
	// allWorkTokens is a counter that maintains the total number of existing workTokens. This is
	// used to release more tokens when the active work queue drops below a certain threshold.
	allWorkTokens atomic.Int32
	// nextTokensChan is a buffered channel that releases the priority tokens to the sync manager.
	nextTokensChan      chan [priorityLevels]PriorityToken
	nextRescheduledTime [priorityLevels]time.Time
	rescheduleWorkMu    sync.Mutex
	nextSubmissionIds   [priorityLevels]string
}

func NewScheduler[T any](ctx context.Context, log *zap.Logger, queue chan T, opts ...schedulerOpt) (s *Scheduler, close func() error) {
	cfg := &schedulerConfig{
		nodeName:                   "worker",
		priorityFairness:           STRONG,
		averageWindow:              5 * time.Second,
		releasesPerStatUpdate:      10,
		releaseTokensTick:          100 * time.Millisecond,
		allowedTokensMultiplier:    2.0,
		allowedTokensGrowthRateMax: 0.8,
		allowedTokensMin:           runtime.GOMAXPROCS(0),
	}
	for _, opt := range opts {
		opt(cfg)
	}
	weights := geometricFairnessWeights(cfg.priorityFairness)
	allowedTokensAbsoluteMinimum := int(math.Ceil(1 / weights[priorityLevels-1]))
	cfg.allowedTokensMin = max(allowedTokensAbsoluteMinimum, cfg.allowedTokensMin)
	cfg.nodeName = strings.ReplaceAll(cfg.nodeName, " ", "")

	// Expvars are registered globally and duplication registrations panic. This should never be
	// done except the benchmark tests in rst/sync/internal/workmgr/manager_test.go which create
	// many schedulers.
	schedulerMetricsOnce.Do(func() {
		schedulerCompletedWorkHz = expvar.NewFloat(cfg.nodeName + "_scheduler_completedWorkHz")
		schedulerTokensAllowedHz = expvar.NewFloat(cfg.nodeName + "_scheduler_tokensAllowedHz")
		schedulerPriorityFairnessMode = expvar.NewString(cfg.nodeName + "_priority_fairness_mode")
		schedulerPriorityFairnessMode.Set(cfg.priorityFairness.String())
	})
	s = &Scheduler{
		ctx:              ctx,
		log:              log,
		nextTokensChan:   make(chan [priorityLevels]PriorityToken, 1),
		rescheduleWorkMu: sync.Mutex{},
	}

	for priority := range priorityLevels {
		s.SetNextSubmissionId(submissionIdPriorityStarts[priority], priority)
	}

	releaseTokensTicker := time.NewTicker(cfg.releaseTokensTick)
	close = func() error {
		releaseTokensTicker.Stop()
		return nil
	}

	go func() {
		var (
			queueCapacity       = cap(queue)
			getUsedCapacity     = func() int { return len(queue) }
			alpha               = 2.0 / float64(max(min(cfg.averageWindow.Milliseconds()/cfg.releaseTokensTick.Milliseconds(), 30), 0)+1)
			releaseTokensTickMs = float64(cfg.releaseTokensTick.Milliseconds())
			updateStats         = s.getUpdateStatsFunc(cfg.allowedTokensMultiplier, cfg.allowedTokensMin, cfg.allowedTokensGrowthRateMax, alpha)
			distributeTokens    = s.getDistributeTokensFunc(weights)
			// accumulator gathers the tokens granted per stats window and dribbles them out
			// on each release tick so we can make small releases without changing the total
			// allowance; it resets whenever updateStats runs to avoid biasing the next window.
			accumulator              = 0.0
			allowedTokensPerMs       = 0.0
			totalTokensDistributed   = 0
			previousTime             = time.Now()
			shouldReportIdleStatus   = true
			shouldReportActiveStatus = false
		)

		updateCounter := 0
		for {
			select {
			case <-s.ctx.Done():
				return
			case currentTime := <-releaseTokensTicker.C:
				usedCapacity := getUsedCapacity()
				elapsedTimeMs := float64(currentTime.Sub(previousTime).Milliseconds())

				if updateCounter == 0 {
					allowedTokensPerMs = updateStats(currentTime, usedCapacity, totalTokensDistributed)
					accumulator = 0

					if totalTokensDistributed > 0 {
						if shouldReportActiveStatus {
							log.Info(cfg.nodeName + " node is no longer idle")
							shouldReportIdleStatus = true
							shouldReportActiveStatus = false
						}
						totalTokensDistributed = 0
					} else if shouldReportIdleStatus && usedCapacity == 0 && s.allWorkTokens.Load() == 0 {
						log.Info(cfg.nodeName + " node is idle")
						shouldReportIdleStatus = false
						shouldReportActiveStatus = true
					}
				}
				updateCounter++
				if updateCounter == cfg.releasesPerStatUpdate {
					updateCounter = 0
				}

				previousTime = currentTime
				target := allowedTokensPerMs * elapsedTimeMs
				current := float64(usedCapacity) * (elapsedTimeMs / releaseTokensTickMs)
				accumulator += target - current
				if accumulator < 1 {
					continue
				}

				allowedTokens := int(accumulator)
				priorityTokens, tokensDistributed := distributeTokens(min(allowedTokens, queueCapacity-usedCapacity))
				if tokensDistributed > 0 {
					select {
					case s.nextTokensChan <- priorityTokens:
						totalTokensDistributed += tokensDistributed
						accumulator -= float64(tokensDistributed)
					default:
						// tokensReleased channel buffer is full so just keep accumulate tokens.
					}
				}
			}
		}
	}()

	return
}

// GetPriorityLevels returned the total number of priorities.
func (s *Scheduler) GetPriorityLevels() int {
	return priorityLevels
}

// GetNextPriorityTokenChan streams tokens distributed to each priority. The results rotate the
// priority order so no one priority will always be first.
func (s *Scheduler) GetNextPriorityTokenChan() chan [priorityLevels]PriorityToken {
	return s.nextTokensChan
}

// IsNextRescheduledTimeExpired returns whether the next rescheduled work request has expired. Note
// that this will only return true once for each reschedule time set in SetNextRescheduledTime. It
// is the responsibility of the caller to set the next time if there is one.
func (s *Scheduler) IsNextRescheduledTimeExpired(priority int) bool {
	s.rescheduleWorkMu.Lock()
	defer s.rescheduleWorkMu.Unlock()

	if s.rescheduledWork[priority].Load() > 0 && time.Now().After(s.nextRescheduledTime[priority]) {
		s.nextRescheduledTime[priority] = time.Time{}
		return true
	}
	return false
}

// SetNextRescheduledTime sets nextRescheduledTime when the previous time has already been triggered
// or ExecuteAfter is before nextRescheduledTime. Zero time indicates an invalid state and will
// accept any new ExecuteAfter time when set.
func (s *Scheduler) SetNextRescheduledTime(ExecuteAfter time.Time, priority int) {
	s.rescheduleWorkMu.Lock()
	defer s.rescheduleWorkMu.Unlock()

	if s.nextRescheduledTime[priority].IsZero() || s.nextRescheduledTime[priority].After(ExecuteAfter) {
		s.nextRescheduledTime[priority] = ExecuteAfter
	}
}

// AddRescheduleWorkToken adds a rescheduleWorkToken and sets the next
// check time if needed.
func (s *Scheduler) AddRescheduleWorkToken(submissionId string, ExecuteAfter time.Time) {
	s.rescheduleWorkMu.Lock()
	defer s.rescheduleWorkMu.Unlock()

	priority := s.AddWorkToken(submissionId)
	s.rescheduledWork[priority].Add(1)
	if s.nextRescheduledTime[priority].IsZero() || s.nextRescheduledTime[priority].After(ExecuteAfter) {
		s.nextRescheduledTime[priority] = ExecuteAfter
	}
}

func (s *Scheduler) RemoveRescheduledWorkToken(submissionId string) {
	priority := s.RemoveWorkToken(submissionId)
	s.rescheduledWork[priority].Add(-1)
}

func (s *Scheduler) GetPriorityRange(priority int) (start string, stop string) {
	return submissionIdPriorityRange(priority)
}

func (s *Scheduler) GetNextSubmissionId(priority int) string {
	next := s.nextSubmissionIds[priority]
	if next == "" {
		next = submissionIdPriorityStarts[priority]
		s.SetNextSubmissionId(next, priority)
	}
	return next
}
func (s *Scheduler) SetNextSubmissionId(submissionId string, priority int) {
	if submissionIdPriority(submissionId) != int32(priority) {
		s.log.Error("failed to set next submissionId", zap.Error(fmt.Errorf("invalid priority submissionId")), zap.String("submissionId", submissionId), zap.Int("priority", priority+1))
		return
	}
	s.nextSubmissionIds[priority] = submissionId
}

// AddWorkToken(submissionID) tells the scheduler about a submission in the journal that is eligible
// to be scheduled. It should be called whenever a WR is created, rediscovered on startup, or
// rescheduled. The scheduler decodes the submission ID to determine the priority and increment that
// bucket's token count. These counts are used to keep track of pending work at each priority to
// ensure no priority queue is starved. Tokens represent work that is ready but not yet dispatched
// to a worker.
func (s *Scheduler) AddWorkToken(submissionId string) int32 {
	priority := submissionIdPriority(submissionId)
	s.workTokens[priority].Add(1)
	s.allWorkTokens.Add(1)
	return priority
}

// RemoveWorkToken(submissionID) is called once a work assignment has been added to the active work
// queue (not when it actually completes). This tells the scheduler a request at the given priority
// has been handed to a worker, allowing it to internally adjust how it assigns new work.
func (s *Scheduler) RemoveWorkToken(submissionId string) int32 {
	priority := submissionIdPriority(submissionId)
	s.workTokens[priority].Add(-1)
	s.allWorkTokens.Add(-1)
	return priority
}

func (s *Scheduler) getUpdateStatsFunc(
	allowedTokensMultiplier float64,
	allowedTokensMinimum int,
	allowedTokensGrowthRateMaximum float64,
	alpha float64,
) func(time.Time, int, int) float64 {
	var (
		averageCompletedWorkPerMs = 0.0
		averageDurationMs         = 0.0
		previousTime              = time.Time{}
		previousUsedCapacity      = 0
	)

	return func(currentTime time.Time, usedCapacity int, tokensDistributed int) float64 {
		if previousTime.IsZero() {
			previousTime = currentTime
			return 0
		}
		elapsedTimeMs := float64(currentTime.Sub(previousTime).Milliseconds())
		if averageDurationMs == 0 {
			averageDurationMs = elapsedTimeMs
		} else {
			averageDurationMs = alpha*elapsedTimeMs + (1-alpha)*averageDurationMs
		}
		previousTime = currentTime
		minTokensMs := float64(allowedTokensMinimum) / averageDurationMs
		completedWork := float64(previousUsedCapacity + tokensDistributed - usedCapacity)
		completedWorkPerMs := completedWork / elapsedTimeMs
		previousUsedCapacity = usedCapacity

		// Initial value or when idle for a while.
		if averageCompletedWorkPerMs <= tolerance {
			averageCompletedWorkPerMs = completedWorkPerMs
			s.log.Debug("scheduler allowance",
				zap.Int("usedCapacity", usedCapacity),
				zap.Int("tokensAllowedMs", int(RoundToMillis(minTokensMs))),
			)
			return minTokensMs
		}

		var growthFactor float64 = 0
		growthDenominator := math.Abs(averageCompletedWorkPerMs)
		if growthDenominator > tolerance {
			growthFactor = (completedWorkPerMs - averageCompletedWorkPerMs) / growthDenominator
			if growthFactor > allowedTokensGrowthRateMaximum {
				growthFactor = allowedTokensGrowthRateMaximum
			} else if growthFactor < -allowedTokensGrowthRateMaximum {
				growthFactor = -allowedTokensGrowthRateMaximum
			}
		}

		averageCompletedWorkPerMs = alpha*completedWorkPerMs + (1-alpha)*averageCompletedWorkPerMs
		if averageCompletedWorkPerMs*averageDurationMs < 1 {
			averageCompletedWorkPerMs = 0
			return minTokensMs
		}

		tokensAllowedPerMs := allowedTokensMultiplier * (1 + growthFactor) * averageCompletedWorkPerMs
		s.log.Debug("scheduler allowance",
			zap.Int("usedCapacity", usedCapacity),
			zap.Int("tokensAllowedMs", int(RoundToMillis(tokensAllowedPerMs))),
		)

		schedulerCompletedWorkHz.Set(completedWorkPerMs * 1000)
		schedulerTokensAllowedHz.Set(tokensAllowedPerMs * 1000)

		return max(tokensAllowedPerMs, minTokensMs)
	}
}

// getDistributeTokensFunc returns a function that distributes tokens to each priority in
// round-robin fashion. Each round distributes according to dynamic weights based on which
// priorities have work.
func (s *Scheduler) getDistributeTokensFunc(weights [priorityLevels]float64) func(int) ([priorityLevels]PriorityToken, int) {
	var (
		nextPriority     = getNextPriorityFunc()
		tokenAccumulator = [priorityLevels]float64{}
	)

	return func(tokensAllowed int) (priorityTokens [priorityLevels]PriorityToken, tokensDistributed int) {
		workTokens := [priorityLevels]int{}
		for priority := range priorityLevels {
			workTokens[priority] = int(s.workTokens[priority].Load())
		}

		tokensLeft := tokensAllowed
	finished:
		for tokensLeft > 0 {
			// Determine the weight normalizer for priorities with work.
			normalizer := 1.0
			for priority := range priorityLevels {
				if workTokens[priority] < 1 {
					normalizer -= weights[priority]
				}
			}
			if normalizer < tolerance {
				break finished
			}

			// Calculate the greatest portion of tokensAllowed that can be distributed to satisfy
			// the priority with the least work. There will be at most one loop per priority.
			nextPortion := 0
			for priority := range priorityLevels {
				if workTokens[priority] < 1 {
					continue
				}
				weight := weights[priority] / normalizer
				portion := min(tokensLeft, int(math.Ceil(float64(workTokens[priority])/weight)))
				if nextPortion == 0 || portion < nextPortion {
					nextPortion = portion
				}
			}

			// Distribute the nextPortion based on the dynamic weights to the priorities.
			index := -1
			for priority, ok := nextPriority(); ok; priority, ok = nextPriority() {
				index++
				priorityTokens[index].Priority = priority
				priorityTokens[index].Tokens = 0

				if workTokens[priority] < 1 {
					continue
				}

				weight := weights[priority] / normalizer
				share := weight * float64(nextPortion)
				tokenAccumulator[priority] += share

				grant := min(tokensLeft, workTokens[priority], int(tokenAccumulator[priority]))
				if grant < 1 {
					continue
				}
				priorityTokens[index].Tokens += grant
				tokensLeft -= grant
				workTokens[priority] -= grant
				tokenAccumulator[priority] -= float64(grant)
			}
		}
		tokensDistributed = tokensAllowed - tokensLeft
		tokensUnused := tokensLeft
		s.log.Debug("token scheduler distribution",
			zap.Int("tokensDistributed", tokensDistributed),
			zap.Int("tokensUnused", tokensUnused),
			zap.Any("priorityTokens", priorityTokens))
		return
	}
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
var submissionIdPriorityStarts = [priorityLevels]string{"0000000000000", "4000000000000", "a000000000000", "e000000000000", "i000000000000"}
var submissionIdPriorityStops = [priorityLevels]string{"4000000000000", "a000000000000", "e000000000000", "i000000000000", "m000000000000"}

func submissionIdPriorityRange(priority int) (start, stop string) {
	return submissionIdPriorityStarts[priority], submissionIdPriorityStops[priority]
}

func CreateSubmissionId(baseKey string, workRequestPriority int32) (string, int32) {
	priority := workRequestPriority - 1
	if priority < 0 || priority > priorityLevels-1 {
		priority = 2
	}

	workRequestPriority = priority + 1
	leadByte := baseKey[0] + submissionIdPriorityOffsetTable[priority]
	return string(leadByte) + baseKey[1:], workRequestPriority
}

func IncrementSubmissionId(key string) (string, int32, error) {
	workRequestPriority := submissionIdPriority(key) + 1
	value, err := strconv.ParseUint(submissionBaseKey(key), 36, 64)
	if err != nil {
		return "", 0, fmt.Errorf("unable to cast last submission ID to an integer '%s': %w", key, err)
	}

	baseKey := fmt.Sprintf("%013s", strconv.FormatUint(value+1, 36))
	submissionId, priority := CreateSubmissionId(baseKey, workRequestPriority)
	return submissionId, priority, nil
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
// every level eventually gets a turn to go first. WARNING: Do not interrupt cycles otherwise
// counter and id will not reset.
func getNextPriorityFunc() func() (int, bool) {
	counter := 0
	id := -1
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

type geometricRatio float64

const (
	AGGRESSIVE geometricRatio = 0.50  // [51.6 25.8 12.9 06.4 03.2]
	STRONG     geometricRatio = 0.667 // [38.3 25.5 17.0 11.3 07.5]
	BALANCED   geometricRatio = 0.75  // [32.7 24.5 18.4 13.8 10.3]
	GENTLE     geometricRatio = 0.85  // [26.9 22.9 19.4 16.5 14.0]
	FAIR       geometricRatio = 0.90  // [24.4 21.9 19.7 17.8 16.0]
)

func (r geometricRatio) String() string {
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
func geometricFairnessWeights(ratio geometricRatio) (weights [priorityLevels]float64) {
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
