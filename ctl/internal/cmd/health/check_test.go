package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckCfgValidate(t *testing.T) {
	tests := []struct {
		name          string
		watchInterval time.Duration
		expectedErr   string
	}{
		{
			name:          "positive watch interval is valid",
			watchInterval: time.Second,
		},
		{
			name:          "zero watch interval is rejected",
			watchInterval: 0,
			expectedErr:   "--watch must be greater than 0 (got 0s)",
		},
		{
			name:          "negative watch interval is rejected",
			watchInterval: -time.Second,
			expectedErr:   "--watch must be greater than 0 (got -1s)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCfg{watchInterval: tc.watchInterval}.validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.expectedErr, err.Error())
		})
	}
}
