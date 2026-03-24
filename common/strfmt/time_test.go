package strfmt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExpirationString(t *testing.T) {
	assert.Equal(t, "expires in 365 days", ExpirationString(time.Hour*24*365))
	assert.Equal(t, "expires in 364 days", ExpirationString(time.Hour*24*364))

	// 24 hours exactly
	assert.Equal(t, "expires in 1 day", ExpirationString(time.Minute*1440))

	// 24 hours - 1 minute
	assert.Equal(t, "expires in 24 hours", ExpirationString(time.Minute*1439))

	assert.Equal(t, "expires in 2 hours", ExpirationString(time.Minute*119))
	assert.Equal(t, "expires in 2 hours", ExpirationString(time.Minute*90))
	assert.Equal(t, "expires in 1 hour", ExpirationString(time.Minute*89))
	assert.Equal(t, "expires in 1 hour", ExpirationString(time.Minute*60))
	assert.Equal(t, "expires in less than an hour", ExpirationString(time.Minute*59))

	assert.Equal(t, "expired less than an hour ago", ExpirationString(0))
	assert.Equal(t, "expired less than an hour ago", ExpirationString(-1*time.Minute*59))
	assert.Equal(t, "expired 1 hour ago", ExpirationString(-1*time.Minute*60))
	assert.Equal(t, "expired 1 hour ago", ExpirationString(-1*time.Minute*61))
	assert.Equal(t, "expired 2 hours ago", ExpirationString(-1*time.Minute*90))
	assert.Equal(t, "expired 24 hours ago", ExpirationString(-1*time.Minute*1439))
	assert.Equal(t, "expired 1 day ago", ExpirationString(-1*time.Minute*1440))
}
