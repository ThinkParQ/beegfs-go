//go:build linux

package iotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/xattr"
)

const (
	invalDefaultRounds       = 1000
	invalDefaultRoundTimeout = 10 * time.Second
	invalDefaultReadyTimeout = 30 * time.Second
	invalDefaultBlockSize    = int64(4096)

	// invalControlPort carries all per-round leader/follower coordination
	// (ready handshake, round announcements, observation reports) over a
	// plain TCP connection instead of files on cfg.Path. Round-signal files
	// used to live on the mount under test, which meant raising a cache TTL
	// to probe for a regression also made the coordination signal itself go
	// stale by the same amount: a follower's single revalidation of that
	// directory could refresh both the round file's existence AND the test
	// file's mutated attribute in one round-trip, absorbing the real
	// invalidation delay into the (unmeasured) round-file wait and reporting
	// near-zero latency for a case that was actually slow. This is a
	// well-known failure mode in coordination-channel design generally:
	// coordination must not ride the same cache/TTL regime as the thing
	// being measured.
	//
	// Deliberately above 60999: Linux's default ephemeral/outgoing port
	// range (net.ipv4.ip_local_port_range) commonly runs 32768-60999, and a
	// fixed listener port inside that range can transiently collide with an
	// unrelated outgoing connection's source port under connection churn.
	// 61999 sits in IANA's dynamic/private range (49152-65535) reserved for
	// exactly this kind of ephemeral-but-not-OS-assigned use.
	invalControlPort = 61999

	invalLockFile    = ".inval-leader.lock"
	invalTestFile    = "inval-test"
	invalRenameFileA = "inval-rename-a"
	invalRenameFileB = "inval-rename-b"
	invalUnlinkFile  = "inval-unlink-test"
	invalXattrName   = "user.inval_marker"
	invalLogFileFmt  = "inval-log-%s-%s.txt" // role (leader/follower), hostname
)

// invalOp identifies the kind of metadata mutation applied in a round.
type invalOp string

const (
	invalOpSize   invalOp = "size"
	invalOpChmod  invalOp = "chmod"
	invalOpMtime  invalOp = "mtime"
	invalOpRename invalOp = "rename"
	invalOpUnlink invalOp = "unlink"
	invalOpXattr  invalOp = "xattr"
)

// invalAllOps lists every operation type. The leader picks one at random
// each round; invalPrintSummary iterates this slice for the per-op breakdown.
var invalAllOps = []invalOp{invalOpSize, invalOpChmod, invalOpMtime, invalOpRename, invalOpUnlink, invalOpXattr}

// errStopped is returned by invalWaitForFile when the stop signal fires,
// allowing callers to distinguish a clean stop from a real timeout.
var errStopped = errors.New("stop signal received")

// InvalTool implements Tool. It tests metadata cache invalidation by electing
// a leader to mutate a file's metadata across multiple rounds, while followers
// measure how quickly they observe each mutation via stat().
type InvalTool struct{}

func (t *InvalTool) Name() string { return "inval" }
func (t *InvalTool) Description() string {
	return "metadata cache-invalidation test; measures observation latency per node"
}

func (t *InvalTool) Run(ctx context.Context, cfg RunConfig) (err error) {
	hostname, err := invalNodeName(cfg)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return fmt.Errorf("create path %s: %w", cfg.Path, err)
	}

	isLeader, leaderHost, err := invalElectLeader(cfg.Path, hostname)
	if err != nil {
		return err
	}
	if isLeader {
		// invalElectLeader already created the O_CREATE|O_EXCL lock file by
		// the time it returns isLeader=true. defer here -- rather than a
		// bare call at the end of invalRunLeader's round loop -- so a
		// mid-run error on any round, or even a failure to create the
		// per-node log file just below, doesn't leave a stale lock file
		// behind. A stale lock blocks every future run on this path: the
		// next invocation's O_CREATE|O_EXCL would see a "leader" that
		// already won and every node would wait forever as a follower.
		defer invalCleanup(cfg.Path)
	}

	role := "follower"
	if isLeader {
		role = "leader"
	}
	logPath := filepath.Join(cfg.Path, fmt.Sprintf(invalLogFileFmt, role, hostname))
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("create log file: %w", err)
	}
	// The per-node results log (round-by-round latency stats) is this test's
	// actual diagnostic output -- losing the tail of it to an unflushed
	// Close silently would defeat the point of running inval at all.
	defer func() {
		if closeErr := logFile.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	fmt.Printf("inval: writing results to %s\n", logPath)

	if isLeader {
		return invalRunLeader(ctx, cfg, leaderHost, logFile)
	}
	return invalRunFollower(ctx, cfg, leaderHost, logFile)
}

// ── Types ─────────────────────────────────────────────────────────────────────

// invalCacheStats mirrors the counters in /proc/fs/beegfs/*/inode_cache_stats.
// Read before and after polling so each round can report the delta invalidation count.
type invalCacheStats struct {
	Hits          uint64
	Misses        uint64
	Invalidations uint64
}

// invalReady is sent once by each follower immediately after connecting to
// the leader's control-channel listener, identifying itself by hostname.
type invalReady struct {
	Node string `json:"node"`
}

// invalRound is sent by the leader over each follower's control connection
// immediately after it mutates the test file. Followers decode this to learn
// which operation was applied, the expected post-mutation state, and the
// timestamp of the mutation. Only the field(s) relevant to Op are populated.
type invalRound struct {
	Round             int       `json:"round"`
	Op                invalOp   `json:"op"`
	ExpectedSize      int64     `json:"expectedSize,omitempty"`
	ExpectedMode      uint32    `json:"expectedMode,omitempty"`
	ExpectedMtime     time.Time `json:"expectedMtime,omitempty"`
	OldName           string    `json:"oldName,omitempty"`
	ExpectedName      string    `json:"expectedName,omitempty"`
	ExpectedExists    bool      `json:"expectedExists,omitempty"` // invalOpUnlink only
	ExpectedXattr     string    `json:"expectedXattr,omitempty"`  // invalOpXattr only
	MutatedAt         time.Time `json:"mutatedAt"`
	ExpectedFollowers int       `json:"expectedFollowers"`
}

// invalObservation is sent back by each follower over its control connection
// once it detects the expected post-mutation state via stat(). The leader
// decodes these to compute per-round latency and detect followers that
// timed out.
type invalObservation struct {
	Round      int       `json:"round"`
	Op         invalOp   `json:"op"`
	Node       string    `json:"node"`
	ObservedAt time.Time `json:"observedAt,omitempty"`
	LatencyMs  int64     `json:"latencyMs"`
	Timeout    bool      `json:"timeout,omitempty"`
}

// invalRoundResult aggregates all observations the leader collected for a single
// round. Missed lists follower hostnames that did not respond within the timeout.
type invalRoundResult struct {
	Round        int
	Op           invalOp
	Observations []invalObservation
	Missed       []string
}

func (r invalRoundResult) summary() string {
	if len(r.Missed) > 0 {
		return fmt.Sprintf("%d observed, %d missed: %s", len(r.Observations), len(r.Missed), strings.Join(r.Missed, ", "))
	}
	if len(r.Observations) == 0 {
		return "no observations"
	}
	var lats []int64
	for _, o := range r.Observations {
		lats = append(lats, o.LatencyMs)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	return fmt.Sprintf("%d observed, min=%dms max=%dms", len(r.Observations), lats[0], lats[len(lats)-1])
}

// ── Leader election ───────────────────────────────────────────────────────────

// invalNodeName returns this node's identity for leader/follower
// coordination: cfg.NodeName if the framework set one (multi-node runs,
// where it's the --nodes name the operator typed and is therefore
// resolvable by the other participating nodes), otherwise os.Hostname().
// Raw os.Hostname() is not guaranteed resolvable by other nodes -- e.g. a
// VM's real hostname can differ from the DNS/hosts alias other nodes know
// it by -- so anything dialed over invalControlPort must use this, not
// os.Hostname() directly.
func invalNodeName(cfg RunConfig) (string, error) {
	if cfg.NodeName != "" {
		return cfg.NodeName, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("get hostname: %w", err)
	}
	return hostname, nil
}

// invalElectLeader attempts to become leader via O_CREATE|O_EXCL on the lock
// file. Returns (true, hostname, nil) for the leader; (false, leaderHostname,
// nil) for followers. Includes paranoia checks on both sides.
func invalElectLeader(path, hostname string) (isLeader bool, leaderHost string, err error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return false, "", fmt.Errorf("create path %s: %w", path, err)
	}

	lockPath := filepath.Join(path, invalLockFile)

	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err == nil {
		_, writeErr := fmt.Fprint(f, hostname)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			// If the remove also fails, a stray lock file would block every
			// future leader election on this path until someone notices and
			// removes it by hand -- fold that into the returned error rather
			// than losing it.
			removeErr := os.Remove(lockPath)
			return false, "", fmt.Errorf("write leader lock: %w", errors.Join(writeErr, closeErr, removeErr))
		}
		// Paranoia: read back and verify our hostname is there.
		data, err := os.ReadFile(lockPath)
		if err != nil {
			removeErr := os.Remove(lockPath)
			return false, "", fmt.Errorf("verify leader lock: %w", errors.Join(err, removeErr))
		}
		if got := strings.TrimSpace(string(data)); got != hostname {
			removeErr := os.Remove(lockPath)
			return false, "", errors.Join(fmt.Errorf("leader lock verification failed: wrote %q, read back %q", hostname, got), removeErr)
		}
		return true, hostname, nil
	}

	if !os.IsExist(err) {
		return false, "", fmt.Errorf("create leader lock: %w", err)
	}

	// We're a follower. Poll until the lock file is written (brief window).
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(lockPath)
		if err == nil {
			if h := strings.TrimSpace(string(data)); h != "" {
				leaderHost = h
				break
			}
		}
		if time.Now().After(deadline) {
			return false, "", fmt.Errorf("timeout waiting for leader lock to be populated")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Paranoia: the lock file must not contain our hostname.
	if leaderHost == hostname {
		return false, "", fmt.Errorf("leader lock contains our own hostname %q but we did not create it — filesystem consistency error", hostname)
	}

	return false, leaderHost, nil
}

func invalVerifyLock(path, expectedHostname string) error {
	data, err := os.ReadFile(filepath.Join(path, invalLockFile))
	if err != nil {
		return fmt.Errorf("leader lock read failed: %w", err)
	}
	if got := strings.TrimSpace(string(data)); got != expectedHostname {
		return fmt.Errorf("leader lock now contains %q (expected %q) — leadership conflict", got, expectedHostname)
	}
	return nil
}

// ── Leader ────────────────────────────────────────────────────────────────────

func invalRunLeader(ctx context.Context, cfg RunConfig, hostname string, log io.Writer) error {
	testFile := filepath.Join(cfg.Path, invalTestFile)
	renameFile := filepath.Join(cfg.Path, invalRenameFileA)

	// Create test files for followers to stat. invalRenameFileA is the
	// initial name of the rename-test file; rename rounds toggle it between
	// invalRenameFileA and invalRenameFileB.
	if err := os.WriteFile(testFile, []byte{}, 0644); err != nil {
		return fmt.Errorf("create test file: %w", err)
	}
	if err := os.WriteFile(renameFile, []byte{}, 0644); err != nil {
		return fmt.Errorf("create rename test file: %w", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Path, invalUnlinkFile), []byte{}, 0644); err != nil {
		return fmt.Errorf("create unlink test file: %w", err)
	}

	// Probe for user-extended-attribute support. If unsupported, drop
	// invalOpXattr from this run's op set — otherwise xattr.Set below would
	// abort the whole run on the first xattr round.
	ops := invalAllOps
	if err := xattr.Set(testFile, invalXattrName, []byte("0"), 0); err != nil {
		fmt.Fprintf(log, "[leader:%s] user xattrs not supported (%v); skipping %q op\n", hostname, err, invalOpXattr)
		ops = make([]invalOp, 0, len(invalAllOps)-1)
		for _, o := range invalAllOps {
			if o != invalOpXattr {
				ops = append(ops, o)
			}
		}
	} else {
		_ = xattr.Remove(testFile, invalXattrName)
	}

	// Open the off-mount control channel and wait for followers to connect.
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", invalControlPort))
	if err != nil {
		return fmt.Errorf("listen on control port %d: %w", invalControlPort, err)
	}
	defer ln.Close()

	fmt.Fprintf(log, "[leader:%s] waiting up to %s for followers on port %d...\n", hostname, invalDefaultReadyTimeout, invalControlPort)
	followers := invalAcceptFollowers(ctx, ln, cfg.StopSignal)
	if cfg.StopSignal != nil && cfg.StopSignal.Load() {
		return nil
	}
	if len(followers) == 0 {
		return fmt.Errorf("[leader] no followers registered — cannot run inval test")
	}
	defer func() {
		for _, fc := range followers {
			fc.conn.Close()
		}
	}()
	names := make([]string, 0, len(followers))
	for n := range followers {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintf(log, "[leader:%s] %d follower(s): %s\n", hostname, len(followers), strings.Join(names, ", "))

	var results []invalRoundResult
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	renameToB := false   // tracks current name of the rename-test file
	unlinkExists := true // tracks whether the unlink-test file currently exists

	for round := 1; round <= invalDefaultRounds; round++ {
		if ctx.Err() != nil || (cfg.StopSignal != nil && cfg.StopSignal.Load()) {
			break
		}

		if err := invalVerifyLock(cfg.Path, hostname); err != nil {
			return err
		}

		op := ops[rng.Intn(len(ops))]
		rd := invalRound{Round: round, Op: op, ExpectedFollowers: len(followers)}

		switch op {
		case invalOpSize:
			info, err := os.Stat(testFile)
			if err != nil {
				return fmt.Errorf("round %d: stat before truncate: %w", round, err)
			}
			cur := info.Size()
			delta := int64(rng.Intn(3)+1) * invalDefaultBlockSize // 1-3 blocks
			if cur > 0 && rng.Intn(2) == 0 {
				if delta > cur {
					delta = cur
				}
				rd.ExpectedSize = cur - delta
			} else {
				rd.ExpectedSize = cur + delta
			}
			if err := os.Truncate(testFile, rd.ExpectedSize); err != nil {
				return fmt.Errorf("round %d: truncate: %w", round, err)
			}
		case invalOpChmod:
			// 0600 | round%64 rather than toggling a fixed XOR mask: XOR
			// with a constant is its own inverse, so it used to flip between
			// exactly two mode values with period 2 — a follower whose
			// cached mode was just one round stale would see it match the
			// CURRENT round's expected value, reporting a false "instant
			// visibility" for a real caching regression. Perm() is only 9
			// bits, so this can't be made fully monotone across an
			// unbounded run, but round%64 in the low 6 (group+other) bits
			// raises the alias period from 2 rounds to 64 — far beyond any
			// real cache-staleness window — matching the standard "encode N
			// mod K, K large" fallback for permission-based signals. The
			// owner rw bits (0600) are held
			// fixed rather than folded into the varying range: leader and
			// follower run as the same uid here, and an owner-unreadable or
			// -unwritable mode would break this round's own setxattr/getxattr
			// once invalOpXattr is picked in a later round on the same file.
			rd.ExpectedMode = 0600 | uint32(round%64)
			if err := os.Chmod(testFile, os.FileMode(rd.ExpectedMode)); err != nil {
				return fmt.Errorf("round %d: chmod: %w", round, err)
			}
		case invalOpMtime:
			info, err := os.Stat(testFile)
			if err != nil {
				return fmt.Errorf("round %d: stat before chtimes: %w", round, err)
			}
			rd.ExpectedMtime = info.ModTime().Truncate(time.Second).Add(time.Second)
			if err := os.Chtimes(testFile, rd.ExpectedMtime, rd.ExpectedMtime); err != nil {
				return fmt.Errorf("round %d: chtimes: %w", round, err)
			}
		case invalOpRename:
			oldName, newName := invalRenameFileA, invalRenameFileB
			if renameToB {
				oldName, newName = invalRenameFileB, invalRenameFileA
			}
			renameToB = !renameToB
			if err := os.Rename(filepath.Join(cfg.Path, oldName), filepath.Join(cfg.Path, newName)); err != nil {
				return fmt.Errorf("round %d: rename: %w", round, err)
			}
			rd.OldName = oldName
			rd.ExpectedName = newName
		case invalOpUnlink:
			unlinkPath := filepath.Join(cfg.Path, invalUnlinkFile)
			if unlinkExists {
				if err := os.Remove(unlinkPath); err != nil {
					return fmt.Errorf("round %d: unlink: %w", round, err)
				}
				rd.ExpectedExists = false
			} else {
				if err := os.WriteFile(unlinkPath, []byte{}, 0644); err != nil {
					return fmt.Errorf("round %d: recreate unlink test file: %w", round, err)
				}
				rd.ExpectedExists = true
			}
			unlinkExists = !unlinkExists
		case invalOpXattr:
			rd.ExpectedXattr = strconv.Itoa(round)
			if err := xattr.Set(testFile, invalXattrName, []byte(rd.ExpectedXattr), 0); err != nil {
				return fmt.Errorf("round %d: setxattr: %w", round, err)
			}
		}
		rd.MutatedAt = time.Now()

		result := invalBroadcastRound(followers, round, rd, invalDefaultRoundTimeout, log)
		result.Op = op
		results = append(results, result)
		cfg.TotalOps.Add(1)

		fmt.Fprintf(log, "[leader:%s] round %d (%s): %s\n", hostname, round, op, result.summary())
	}

	invalPrintSummary(log, len(followers), results)
	return nil
}

// invalCleanup removes the leader lock left behind by a run so a
// subsequent run starts from a clean directory. Called via defer in Run
// (covering every exit path, not just clean completion of the round loop)
// as soon as election confirms this node is the leader and the lock file
// therefore already exists. Best-effort: errors go to stderr (the run's own
// log file may already be closed or never created, depending on which exit
// path got here) but never fail the run. Test data files (inval-test,
// inval-rename-*, and the per-node log files) are left in place. Round,
// observation, and ready signaling no longer touch cfg.Path at all (see
// invalControlPort), so there is nothing else to clean up here.
func invalCleanup(path string) {
	if err := os.Remove(filepath.Join(path, invalLockFile)); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "[leader] cleanup: remove %s: %v\n", invalLockFile, err)
	}
}

// invalFollowerConn is the leader's handle to one follower's control
// connection: a persistent TCP connection, held open for the life of the
// run, carrying round announcements out and observation reports back.
type invalFollowerConn struct {
	conn net.Conn
	enc  *json.Encoder
	dec  *json.Decoder
}

// invalAcceptFollowers accepts control-channel connections for up to
// invalDefaultReadyTimeout, enforcing a 5-second minimum wait so that
// followers racing the leader still have time to connect. Each connection's
// first message must be an invalReady identifying the follower's hostname.
func invalAcceptFollowers(ctx context.Context, ln net.Listener, stopSig *atomic.Bool) map[string]*invalFollowerConn {
	followers := make(map[string]*invalFollowerConn)
	deadline := time.Now().Add(invalDefaultReadyTimeout)
	minWait := time.Now().Add(5 * time.Second)

	type accepted struct {
		conn net.Conn
		err  error
	}
	acceptCh := make(chan accepted)
	go func() {
		for {
			c, err := ln.Accept()
			acceptCh <- accepted{c, err}
			if err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil || (stopSig != nil && stopSig.Load()) {
			return followers
		}
		if time.Now().After(deadline) {
			return followers
		}
		select {
		case a := <-acceptCh:
			if a.err != nil {
				continue
			}
			node, fc, err := invalHandshakeFollower(a.conn)
			if err != nil {
				a.conn.Close()
				continue
			}
			followers[node] = fc
		case <-ticker.C:
			if len(followers) > 0 && time.Now().After(minWait) {
				return followers
			}
		}
	}
}

// invalHandshakeFollower reads the connecting follower's invalReady message
// off conn, bounding the read so a misbehaving client can't hang the leader.
func invalHandshakeFollower(conn net.Conn) (string, *invalFollowerConn, error) {
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return "", nil, err
	}
	dec := json.NewDecoder(conn)
	var ready invalReady
	if err := dec.Decode(&ready); err != nil {
		return "", nil, err
	}
	if ready.Node == "" {
		return "", nil, fmt.Errorf("empty hostname in ready handshake")
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return "", nil, err
	}
	return ready.Node, &invalFollowerConn{conn: conn, enc: json.NewEncoder(conn), dec: dec}, nil
}

// invalBroadcastRound sends rd to every follower over its control
// connection, then collects an invalObservation back from each within
// timeout. Followers whose connection errors or that don't respond in time
// are recorded in result.Missed; one follower's failure does not block or
// abort collection from the others.
func invalBroadcastRound(followers map[string]*invalFollowerConn, round int, rd invalRound, timeout time.Duration, log io.Writer) invalRoundResult {
	result := invalRoundResult{Round: round}

	type res struct {
		node string
		obs  invalObservation
		err  error
	}
	ch := make(chan res, len(followers))

	for node, fc := range followers {
		node, fc := node, fc
		go func() {
			if err := fc.enc.Encode(rd); err != nil {
				ch <- res{node: node, err: err}
				return
			}
			_ = fc.conn.SetReadDeadline(time.Now().Add(timeout + 2*time.Second))
			var obs invalObservation
			err := fc.dec.Decode(&obs)
			_ = fc.conn.SetReadDeadline(time.Time{})
			ch <- res{node: node, obs: obs, err: err}
		}()
	}

	for i := 0; i < len(followers); i++ {
		r := <-ch
		if r.err != nil {
			result.Missed = append(result.Missed, r.node)
			continue
		}
		if r.obs.Round != round {
			// A stale response for an earlier round -- left unread on the
			// connection after that round's own read deadline gave up
			// waiting for it (the timeout+2s margin above) -- was consumed
			// by this round's Decode call instead of a fresh response.
			// There's no cheap way to resync mid-stream (no per-message
			// framing beyond JSON object boundaries), so this is recorded
			// as missed for THIS round rather than silently misattributing
			// stale latency/timestamp data to it. This follower will likely
			// keep showing as missed for the rest of the run -- a visible,
			// honest failure mode instead of silently wrong numbers.
			fmt.Fprintf(log, "[leader] round %d: %s sent a stale response for round %d -- connection desynced, treating as missed\n",
				round, r.node, r.obs.Round)
			result.Missed = append(result.Missed, r.node)
			continue
		}
		result.Observations = append(result.Observations, r.obs)
	}
	sort.Strings(result.Missed)
	return result
}

// ── Follower ──────────────────────────────────────────────────────────────────

func invalRunFollower(ctx context.Context, cfg RunConfig, leaderHost string, log io.Writer) error {
	hostname, err := invalNodeName(cfg)
	if err != nil {
		return err
	}
	testFile := filepath.Join(cfg.Path, invalTestFile)

	fmt.Fprintf(log, "[follower:%s] leader is %s\n", hostname, leaderHost)

	// Wait for the test file to appear (leader creates it before opening its
	// control-channel listener).
	if err := invalWaitForFile(ctx, testFile, 30*time.Second, cfg.StopSignal); err != nil {
		if errors.Is(err, errStopped) {
			return nil
		}
		return fmt.Errorf("test file never appeared: %w", err)
	}

	// Stat the files to populate the inode/dentry caches.
	if _, err := os.Stat(testFile); err != nil {
		return fmt.Errorf("initial stat: %w", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Path, invalRenameFileA)); err != nil {
		return fmt.Errorf("initial stat of rename file: %w", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Path, invalUnlinkFile)); err != nil {
		return fmt.Errorf("initial stat of unlink file: %w", err)
	}

	// Connect to the leader's off-mount control channel and register ready.
	// The listener may not be up the instant testFile appears, so retry
	// briefly rather than treating an immediate connection failure as fatal.
	conn, err := invalDialLeader(ctx, leaderHost, cfg.StopSignal)
	if err != nil {
		if errors.Is(err, errStopped) {
			return nil
		}
		return fmt.Errorf("connect to leader control channel: %w", err)
	}
	defer conn.Close()
	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	if err := enc.Encode(invalReady{Node: hostname}); err != nil {
		return fmt.Errorf("send ready: %w", err)
	}
	fmt.Fprintf(log, "[follower:%s] ready\n", hostname)

	for round := 1; round <= invalDefaultRounds; round++ {
		if ctx.Err() != nil || (cfg.StopSignal != nil && cfg.StopSignal.Load()) {
			return nil
		}

		// Wait for the round announcement from the leader.
		if err := conn.SetReadDeadline(time.Now().Add(invalDefaultRoundTimeout + 15*time.Second)); err != nil {
			return fmt.Errorf("round %d: set read deadline: %w", round, err)
		}
		var rd invalRound
		if err := dec.Decode(&rd); err != nil {
			return fmt.Errorf("round %d: read round announcement: %w", round, err)
		}
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			return fmt.Errorf("round %d: clear read deadline: %w", round, err)
		}

		// Snapshot procfs before polling.
		before, _ := invalReadCacheStats()

		// Poll until we observe the mutation, using the predicate appropriate
		// for this round's operation type.
		var observedAt time.Time
		switch rd.Op {
		case invalOpSize:
			observedAt, err = invalPollForSize(ctx, testFile, rd.ExpectedSize, invalDefaultRoundTimeout)
		case invalOpChmod:
			observedAt, err = invalPollForMode(ctx, testFile, os.FileMode(rd.ExpectedMode), invalDefaultRoundTimeout)
		case invalOpMtime:
			observedAt, err = invalPollForMtime(ctx, testFile, rd.ExpectedMtime, invalDefaultRoundTimeout)
		case invalOpRename:
			// The interesting cache-invalidation signal is the old name
			// becoming ENOENT, since this node has a stale positive dentry
			// for it (from initial setup or a prior round's priming below).
			observedAt, err = invalPollForGone(ctx, filepath.Join(cfg.Path, rd.OldName), invalDefaultRoundTimeout)
			// Prime the cache for the new name (now the live name of the
			// rename-test file) so the next rename round, which will treat
			// this path as its OldName, has a stale positive dentry to
			// invalidate.
			_, _ = os.Stat(filepath.Join(cfg.Path, rd.ExpectedName))
		case invalOpUnlink:
			unlinkPath := filepath.Join(cfg.Path, invalUnlinkFile)
			if rd.ExpectedExists {
				observedAt, err = invalPollForExists(ctx, unlinkPath, invalDefaultRoundTimeout)
			} else {
				observedAt, err = invalPollForGone(ctx, unlinkPath, invalDefaultRoundTimeout)
			}
		case invalOpXattr:
			observedAt, err = invalPollForXattr(ctx, testFile, invalXattrName, rd.ExpectedXattr, invalDefaultRoundTimeout)
		default:
			err = fmt.Errorf("unknown op %q", rd.Op)
		}

		// Snapshot procfs after.
		after, _ := invalReadCacheStats()
		invalDelta := after.Invalidations - before.Invalidations

		obs := invalObservation{Round: round, Op: rd.Op, Node: hostname}
		if err != nil {
			obs.Timeout = true
			fmt.Fprintf(log, "[follower:%s] round %d (%s): TIMEOUT (invals=%d)\n", hostname, round, rd.Op, invalDelta)
		} else {
			obs.ObservedAt = observedAt
			// LatencyMs is a cross-node delta: the follower's observe time minus the
			// leader's mutate time. It therefore REQUIRES NTP-synchronized clocks
			// (inval does no clock-offset estimation). A negative value is physically
			// impossible — an observation cannot precede its cause — so it means the
			// follower's clock is behind the leader's; clamp to 0 and warn rather than
			// let clock skew poison the min/p50 summary.
			latency := observedAt.Sub(rd.MutatedAt)
			if latency < 0 {
				fmt.Fprintf(log, "[follower:%s] round %d (%s): WARNING negative latency %dms — clock skew; ensure NTP is synced across nodes\n",
					hostname, round, rd.Op, latency.Milliseconds())
				latency = 0
			}
			obs.LatencyMs = latency.Milliseconds()
			fmt.Fprintf(log, "[follower:%s] round %d (%s): %dms (invals=%d)\n", hostname, round, rd.Op, obs.LatencyMs, invalDelta)
		}

		if err := enc.Encode(obs); err != nil {
			return fmt.Errorf("round %d: send observation: %w", round, err)
		}
		cfg.TotalOps.Add(1)
	}

	return nil
}

// invalDialLeader connects to the leader's control-channel listener,
// retrying briefly since the listener may not be up the instant the
// follower observes testFile.
func invalDialLeader(ctx context.Context, leaderHost string, stopSig *atomic.Bool) (net.Conn, error) {
	addr := net.JoinHostPort(leaderHost, strconv.Itoa(invalControlPort))
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if stopSig != nil && stopSig.Load() {
			return nil, errStopped
		}
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timeout connecting to %s: %w", addr, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ── Procfs ────────────────────────────────────────────────────────────────────

// invalReadCacheStats reads inode_cache_stats from the first BeeGFS client
// entry under /proc/fs/beegfs/. Returns a zero struct if not available.
func invalReadCacheStats() (invalCacheStats, error) {
	matches, err := filepath.Glob("/proc/fs/beegfs/*/inode_cache_stats")
	if err != nil || len(matches) == 0 {
		return invalCacheStats{}, fmt.Errorf("inode_cache_stats not found (client mounted with tuneCacheInvalidationInodeWatch?)")
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		return invalCacheStats{}, fmt.Errorf("read %s: %w", matches[0], err)
	}
	var s invalCacheStats
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), ": ", 2)
		if len(parts) != 2 {
			continue
		}
		val, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			continue
		}
		switch parts[0] {
		case "inodeCacheHits":
			s.Hits = val
		case "inodeCacheMisses":
			s.Misses = val
		case "inodeCacheInvalidations":
			s.Invalidations = val
		}
	}
	return s, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func invalWaitForFile(ctx context.Context, path string, timeout time.Duration, stopSig *atomic.Bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if stopSig != nil && stopSig.Load() {
			return errStopped
		}
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %s", filepath.Base(path))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func invalPollForSize(ctx context.Context, path string, expectedSize int64, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	var lastSize int64 = -1
	for {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		info, err := os.Stat(path)
		if err == nil {
			lastSize = info.Size()
			if lastSize == expectedSize {
				return time.Now(), nil
			}
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timeout waiting for size %d (last observed: %d)", expectedSize, lastSize)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func invalPollForMode(ctx context.Context, path string, expectedMode os.FileMode, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	var lastMode os.FileMode
	for {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		info, err := os.Stat(path)
		if err == nil {
			lastMode = info.Mode().Perm()
			if lastMode == expectedMode.Perm() {
				return time.Now(), nil
			}
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timeout waiting for mode %v (last observed: %v)", expectedMode.Perm(), lastMode)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// invalPollForMtime polls until the file's mtime, truncated to second
// precision, matches expectedMtime (which is itself second-truncated by the
// leader). Second precision is used because not all filesystems preserve
// finer-grained mtimes through stat().
func invalPollForMtime(ctx context.Context, path string, expectedMtime time.Time, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	var lastMtime time.Time
	for {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		info, err := os.Stat(path)
		if err == nil {
			lastMtime = info.ModTime().Truncate(time.Second)
			if lastMtime.Equal(expectedMtime) {
				return time.Now(), nil
			}
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timeout waiting for mtime %s (last observed: %s)", expectedMtime, lastMtime)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// invalPollForGone polls until path no longer exists, used to detect a
// rename's old name being invalidated from a stale positive dentry cache.
func invalPollForGone(ctx context.Context, path string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return time.Now(), nil
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timeout waiting for %s to be removed", filepath.Base(path))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// invalPollForExists polls until path exists, used to detect a previously
// removed file's recreation invalidating a stale negative dentry cache.
func invalPollForExists(ctx context.Context, path string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		if _, err := os.Stat(path); err == nil {
			return time.Now(), nil
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timeout waiting for %s to exist", filepath.Base(path))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// invalPollForXattr polls until the named extended attribute on path equals
// expected.
func invalPollForXattr(ctx context.Context, path, name, expected string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	var last string
	for {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		if val, err := xattr.Get(path, name); err == nil {
			last = string(val)
			if last == expected {
				return time.Now(), nil
			}
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timeout waiting for xattr %s=%q (last observed: %q)", name, expected, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ── Summary ───────────────────────────────────────────────────────────────────

func invalPrintSummary(log io.Writer, followerCount int, results []invalRoundResult) {
	var latencies []int64
	missed, total := 0, 0

	for _, r := range results {
		total += followerCount
		missed += len(r.Missed)
		for _, obs := range r.Observations {
			if obs.Timeout {
				missed++
			} else {
				latencies = append(latencies, obs.LatencyMs)
			}
		}
	}

	fmt.Fprintf(log, "\nSummary (%d follower(s), %d rounds):\n", followerCount, len(results))
	fmt.Fprintf(log, "  (latencies are cross-node deltas; meaningful only with NTP-synced clocks)\n")
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Fprintf(log, "  min=%dms  p50=%dms  p95=%dms  p99=%dms  max=%dms\n",
			latencies[0],
			invalPercentile(latencies, 50),
			invalPercentile(latencies, 95),
			invalPercentile(latencies, 99),
			latencies[len(latencies)-1],
		)
	}
	fmt.Fprintf(log, "  Missed invalidations: %d/%d\n", missed, total)

	// Per-operation breakdown.
	perOpLatencies := make(map[invalOp][]int64)
	perOpMissed := make(map[invalOp]int)
	perOpTotal := make(map[invalOp]int)
	for _, r := range results {
		perOpTotal[r.Op] += followerCount
		perOpMissed[r.Op] += len(r.Missed)
		for _, obs := range r.Observations {
			if obs.Timeout {
				perOpMissed[r.Op]++
			} else {
				perOpLatencies[r.Op] = append(perOpLatencies[r.Op], obs.LatencyMs)
			}
		}
	}

	fmt.Fprintf(log, "\nPer-operation breakdown:\n")
	for _, op := range invalAllOps {
		lats := perOpLatencies[op]
		if len(lats) == 0 {
			fmt.Fprintf(log, "  %-7s no observations  missed=%d/%d\n", op, perOpMissed[op], perOpTotal[op])
			continue
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		fmt.Fprintf(log, "  %-7s min=%dms  p50=%dms  p95=%dms  p99=%dms  max=%dms  missed=%d/%d\n",
			op,
			lats[0],
			invalPercentile(lats, 50),
			invalPercentile(lats, 95),
			invalPercentile(lats, 99),
			lats[len(lats)-1],
			perOpMissed[op], perOpTotal[op],
		)
	}
}

func invalPercentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[(len(sorted)-1)*p/100]
}
