package subscriber

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testConfig = []Config{
	{
		Type: "grpc",
		ID:   1,
		Name: "bee-remote",
		GrpcConfig: GrpcConfig{
			Address:    "br-1:1234",
			TlsDisable: true,
		},
	},
	{
		Type: "grpc",
		ID:   2,
		Name: "beegfs-mon",
		GrpcConfig: GrpcConfig{
			Address:           "bm-1:512312",
			TlsDisable:        false,
			DisconnectTimeout: 1,
		},
	},
}

// TestNewSubscribersFromConfig also implicitly tests newSubscriberFromConfig and newBaseSubscriber.
// See the inline notes before adding tests for new/existing subscriber types.
func TestNewSubscribersFromConfig(t *testing.T) {

	configuredSubscribers, err := NewSubscribersFromConfig(testConfig, nil)
	assert.NoError(t, err)

	// The order of expectedSubscribers matters because we will use that to determine which expected subscriber
	// is compared to the parsed output from the above testConfig.
	// In other words the Nth entry in testConfig should line up with the Nth entry below.

	expectedSubscribers := []*Subscriber{
		{
			Config: Config{
				ID:   1,
				Name: "bee-remote",
				Type: "grpc",
				GrpcConfig: GrpcConfig{
					Address:    "br-1:1234",
					TlsDisable: true,
				},
			},
			State: State{
				state: DISCONNECTED,
			},
			Interface: &GRPCSubscriber{
				GrpcConfig: GrpcConfig{
					Address:           "br-1:1234",
					TlsDisable:        true,
					DisconnectTimeout: 30,
				},
			},
		}, {
			Config: Config{
				ID:   2,
				Name: "beegfs-mon",
				Type: "grpc",
				GrpcConfig: GrpcConfig{
					Address:           "bm-1:512312",
					TlsDisable:        false,
					DisconnectTimeout: 1,
				},
			},
			State: State{
				state: DISCONNECTED,
			},
			Interface: &GRPCSubscriber{
				GrpcConfig: GrpcConfig{
					Address:           "bm-1:512312",
					TlsDisable:        false,
					DisconnectTimeout: 1,
				},
			},
		},
	}

	assert.Len(t, configuredSubscribers, len(expectedSubscribers))

	for i, baseSubscriber := range configuredSubscribers {

		assert.Equal(t, newComparableSubscriber(expectedSubscribers[i], &ComparableSubscriber{}), newComparableSubscriber(baseSubscriber, &ComparableSubscriber{}))

		// If support is added for new subscriber types they will need to be added to the type switch.
		// They will also need to be added to the ComparableSubscriber type constraint before using newComparableSubscriber().
		switch baseSubscriber.Interface.(type) {
		case *GRPCSubscriber:
			actualGRPCSubscriber := baseSubscriber.Interface.(*GRPCSubscriber)
			expectedGRPCSubscriber, ok := expectedSubscribers[i].Interface.(*GRPCSubscriber)
			assert.True(t, ok) // If s is the wrong subscriber type, then the expected subscriber type won't match.
			// We use the newComparableXSubscriber() functions provided alongside each subscriber implementation
			// to get a comparable view of the expected and configured subscriber.
			assert.Equal(t, newComparableSubscriber(expectedGRPCSubscriber, &ComparableGRPCSubscriber{}), newComparableSubscriber(actualGRPCSubscriber, &ComparableGRPCSubscriber{}))
		default:
			assert.Fail(t, "unable to determine subscriber type")
		}
	}
}

func TestNewSubscriberAuthSecret(t *testing.T) {
	secret := []byte("secret")
	optIn := Config{Type: "grpc", ID: 1, Name: "remote", GrpcConfig: GrpcConfig{SendAuthSecret: true}}
	optOut := Config{Type: "grpc", ID: 2, Name: "other"}

	s, err := newSubscriberFromConfig(optIn, secret)
	if err != nil {
		t.Fatalf("opt-in with a secret: unexpected error: %v", err)
	}
	if got := s.Interface.(*GRPCSubscriber).authSecret; string(got) != string(secret) {
		t.Errorf("opt-in subscriber holds secret %q, want %q", got, secret)
	}

	s, err = newSubscriberFromConfig(optOut, secret)
	if err != nil {
		t.Fatalf("opt-out: unexpected error: %v", err)
	}
	if got := s.Interface.(*GRPCSubscriber).authSecret; got != nil {
		t.Errorf("opt-out subscriber holds secret %q, want none", got)
	}

	if _, err := newSubscriberFromConfig(optIn, nil); err == nil {
		t.Error("opt-in without a secret: expected an error")
	}
}
