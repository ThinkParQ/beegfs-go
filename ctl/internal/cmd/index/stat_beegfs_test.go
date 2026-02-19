package index

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseBeeGFSStripesReader(t *testing.T) {
	input := strings.Join([]string{
		"2-6948F85E-2\t0\t201\t524288\t4",
		"2-6948F85E-2\t1\t202\t524288\t4",
		"",
	}, "\n")
	stripes, err := parseBeeGFSStripesReader(strings.NewReader(input), "\t")
	assert.NoError(t, err)
	assert.Len(t, stripes, 2)
	assert.Equal(t, 0, stripes[0].Ordinal)
	assert.Equal(t, "201", stripes[0].TargetID)
	assert.Equal(t, 1, stripes[1].Ordinal)
	assert.Equal(t, "202", stripes[1].TargetID)
}

func TestFormatBeeGFSStatOutput_File(t *testing.T) {
	info := beegfsEntryInfo{
		EntryID:           "0-66854319-2",
		ParentID:          "0-668542DD-2",
		OwnerID:           "2",
		EntryType:         "2",
		StripePatternType: "1",
		ChunkSize:         "524288",
		NumTargets:        "4",
	}
	stripes := []beegfsStripeInfo{
		{Ordinal: 0, TargetID: "501"},
		{Ordinal: 1, TargetID: "601"},
		{Ordinal: 2, TargetID: "701"},
		{Ordinal: 3, TargetID: "801"},
	}

	lines := formatBeeGFSStatOutput("system.log", info, stripes)
	expected := []string{
		"path: system.log",
		"Entry type: file",
		"EntryID: 0-66854319-2",
		"ParentID: 0-668542DD-2",
		"Metadata Owner ID: 2",
		"Stripe pattern details:",
		"+ Type: RAID0",
		"+ Chunksize: 524288",
		"+ Number of storage targets: 4",
		" + Target ID 501",
		" + Target ID 601",
		" + Target ID 701",
		" + Target ID 801",
	}
	assert.Equal(t, expected, lines)
}
