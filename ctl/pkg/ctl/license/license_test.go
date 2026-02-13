package license

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTimeToExpire(t *testing.T) {
	_, expireMsg := getTimeToExpirationString(time.Hour * 24 * 365)
	assert.Equal(t, "expires in 365 days", expireMsg)

	_, expireMsg = getTimeToExpirationString(time.Hour * 24 * 364)
	assert.Equal(t, "expires in 364 days", expireMsg)

	// 24 hours exactly
	_, expireMsg = getTimeToExpirationString(time.Minute * 1440)
	assert.Equal(t, "expires in 1 day", expireMsg)

	// 24 hours - 1 minute
	_, expireMsg = getTimeToExpirationString(time.Minute * 1439)
	assert.Equal(t, "expires in 24 hours", expireMsg)

	_, expireMsg = getTimeToExpirationString(time.Minute * 119)
	assert.Equal(t, "expires in 2 hours", expireMsg)

	_, expireMsg = getTimeToExpirationString(time.Minute * 90)
	assert.Equal(t, "expires in 2 hours", expireMsg)

	_, expireMsg = getTimeToExpirationString(time.Minute * 89)
	assert.Equal(t, "expires in 1 hour", expireMsg)

	_, expireMsg = getTimeToExpirationString(time.Minute * 60)
	assert.Equal(t, "expires in 1 hour", expireMsg)

	_, expireMsg = getTimeToExpirationString(time.Minute * 59)
	assert.Equal(t, "expires in less than an hour", expireMsg)

	_, expireMsg = getTimeToExpirationString(0)
	assert.Equal(t, "expired less than an hour ago", expireMsg)

	_, expireMsg = getTimeToExpirationString(-1 * time.Minute * 59)
	assert.Equal(t, "expired less than an hour ago", expireMsg)

	_, expireMsg = getTimeToExpirationString(-1 * time.Minute * 60)
	assert.Equal(t, "expired 1 hour ago", expireMsg)

	_, expireMsg = getTimeToExpirationString(-1 * time.Minute * 61)
	assert.Equal(t, "expired 1 hour ago", expireMsg)

	_, expireMsg = getTimeToExpirationString(-1 * time.Minute * 90)
	assert.Equal(t, "expired 2 hours ago", expireMsg)

	_, expireMsg = getTimeToExpirationString(-1 * time.Minute * 1439)
	assert.Equal(t, "expired 24 hours ago", expireMsg)

	_, expireMsg = getTimeToExpirationString(-1 * time.Minute * 1440)
	assert.Equal(t, "expired 1 day ago", expireMsg)
}
