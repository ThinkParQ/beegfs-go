//go:build linux

// This is a unit test.
//
// Coverage: invalBroadcastRound's round-number correlation check -- a
// follower's stale response for an earlier round (left over from a
// previous round's expired read deadline) must be recorded as missed for
// the current round, not silently misattributed as a real observation.
package iotest

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

// newFakeFollowerConn returns an invalFollowerConn backed by one end of an
// in-memory net.Pipe, plus the other end for the test to act as the
// follower. json.Encoder/Decoder wrap the leader-side end exactly as
// invalAcceptFollowers does for a real connection.
func newFakeFollowerConn(t *testing.T) (*invalFollowerConn, net.Conn) {
	t.Helper()
	leaderSide, followerSide := net.Pipe()
	t.Cleanup(func() {
		_ = leaderSide.Close()
		_ = followerSide.Close()
	})
	fc := &invalFollowerConn{
		conn: leaderSide,
		enc:  json.NewEncoder(leaderSide),
		dec:  json.NewDecoder(leaderSide),
	}
	return fc, followerSide
}

func TestInvalBroadcastRoundFreshResponse(t *testing.T) {
	fc, followerSide := newFakeFollowerConn(t)
	go func() {
		var rd invalRound
		if err := json.NewDecoder(followerSide).Decode(&rd); err != nil {
			return
		}
		_ = json.NewEncoder(followerSide).Encode(invalObservation{Round: rd.Round, Node: "f1", LatencyMs: 42})
	}()

	followers := map[string]*invalFollowerConn{"f1": fc}
	var log strings.Builder
	result := invalBroadcastRound(followers, 5, invalRound{Round: 5}, time.Second, &log)

	if len(result.Missed) != 0 {
		t.Errorf("Missed = %v, want empty", result.Missed)
	}
	if len(result.Observations) != 1 || result.Observations[0].LatencyMs != 42 {
		t.Errorf("Observations = %v, want one observation with LatencyMs=42", result.Observations)
	}
}

// TestInvalBroadcastRoundStaleResponseTreatedAsMissed is a regression test
// for the round-desync bug: a response carrying an earlier round number
// than the one just broadcast -- exactly what a connection stuck with an
// unread backlog produces -- must not be attributed to the current round.
func TestInvalBroadcastRoundStaleResponseTreatedAsMissed(t *testing.T) {
	fc, followerSide := newFakeFollowerConn(t)
	go func() {
		var rd invalRound
		if err := json.NewDecoder(followerSide).Decode(&rd); err != nil {
			return
		}
		// Simulate a stale response for an earlier round arriving during
		// this round's broadcast.
		_ = json.NewEncoder(followerSide).Encode(invalObservation{Round: rd.Round - 1, Node: "f1", LatencyMs: 999})
	}()

	followers := map[string]*invalFollowerConn{"f1": fc}
	var log strings.Builder
	result := invalBroadcastRound(followers, 5, invalRound{Round: 5}, time.Second, &log)

	if len(result.Observations) != 0 {
		t.Errorf("Observations = %v, want empty -- the stale round-4 response must not be attributed to round 5", result.Observations)
	}
	if len(result.Missed) != 1 || result.Missed[0] != "f1" {
		t.Errorf("Missed = %v, want [f1]", result.Missed)
	}
	if !strings.Contains(log.String(), "desynced") {
		t.Errorf("log output = %q, want a mention of the desync", log.String())
	}
}

func TestInvalBroadcastRoundConnectionError(t *testing.T) {
	fc, followerSide := newFakeFollowerConn(t)
	_ = followerSide.Close() // force the leader's Decode to fail immediately

	followers := map[string]*invalFollowerConn{"f1": fc}
	var log strings.Builder
	result := invalBroadcastRound(followers, 1, invalRound{Round: 1}, time.Second, &log)

	if len(result.Observations) != 0 {
		t.Errorf("Observations = %v, want empty", result.Observations)
	}
	if len(result.Missed) != 1 || result.Missed[0] != "f1" {
		t.Errorf("Missed = %v, want [f1]", result.Missed)
	}
}
