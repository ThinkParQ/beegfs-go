package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/watch/internal/metadata"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
)

func TestUpdateAllowed(t *testing.T) {
	currentConfig := AppConfig{
		Metadata: []metadata.Config{
			{
				EventLogTarget:         "/path",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
		},
	}
	newConfig := AppConfig{
		Metadata: []metadata.Config{
			{
				EventLogTarget:         "/path/changed",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
		},
	}
	newConfig2 := AppConfig{
		Metadata: []metadata.Config{
			{
				EventLogTarget:         "/path",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
			{
				EventLogTarget:         "/new/path",
				EventBufferSize:        100,
				EventBufferGCFrequency: 5,
			},
		},
	}
	assert.NoError(t, currentConfig.UpdateAllowed(&currentConfig))
	assert.Error(t, currentConfig.UpdateAllowed(&newConfig))
	assert.Error(t, currentConfig.UpdateAllowed(&newConfig2))
}

func TestValidateSendAuthSecret(t *testing.T) {
	newConfig := func(authDisable bool) *AppConfig {
		c := &AppConfig{
			Metadata: []metadata.Config{{EventLogTarget: "/run/beegfs/eventlog", EventBufferSize: 1, EventBufferGCFrequency: 1}},
			Subscribers: []subscriber.Config{
				{Type: "grpc", ID: 1, Name: "remote", GrpcConfig: subscriber.GrpcConfig{SendAuthSecret: true}},
			},
		}
		c.Management.AuthDisable = authDisable
		return c
	}
	assert.NoError(t, newConfig(false).ValidateConfig())
	assert.ErrorContains(t, newConfig(true).ValidateConfig(), "grpc-send-auth-secret")
}
