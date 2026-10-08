package quota

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Checks which --uids values may be combined with --users. Resolved names are appended to the ID
// list, which only accepts plain numbers, so keywords and ranges have to be rejected.
func TestRejectUnmergeableIds(t *testing.T) {
	names := []string{"alice"}

	for _, ids := range [][]string{nil, {"1000"}, {"0", "4294967295"}} {
		assert.NoError(t, rejectUnmergeableIds("uids", ids, names), "ids %v", ids)
	}

	for _, ids := range [][]string{{"all"}, {"current"}, {"1000-2000"}, {"1000", "all"}, {"-1"}, {"4294967296"}, {""}} {
		assert.Error(t, rejectUnmergeableIds("uids", ids, names), "ids %v", ids)
		// The same values are ordinary selectors when no name is given, and stay accepted.
		assert.NoError(t, rejectUnmergeableIds("uids", ids, nil), "ids %v", ids)
	}
}

// Checks that appendNames leaves both ID lists alone when no names were given.
func TestAppendNamesWithoutNames(t *testing.T) {
	uids, gids := []string{"all"}, []string{"1000-2000"}
	require.NoError(t, appendNames(&uids, nil, &gids, nil, true))
	assert.Equal(t, []string{"all"}, uids)
	assert.Equal(t, []string{"1000-2000"}, gids)
}

// Checks that an invalid combination is rejected before any name is looked up. "root" always
// resolves, so if the check ran after the lookup this would return no error.
func TestAppendNamesRejectsBeforeResolving(t *testing.T) {
	uids, gids := []string{"all"}, []string{}
	err := appendNames(&uids, []string{"root"}, &gids, nil, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--uids")
	assert.Equal(t, []string{"all"}, uids, "the ID list must be left untouched when rejected")
}
