package util

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLEncodeSignMap(t *testing.T) {
	fsUUID := "019ba41c-a5d6-7d68-a1ec-d3c8c0f04018"
	getArgs, err := URLEncodeSignMap(map[string]string{
		"capacity":    strconv.Itoa(1000),
		"num_meta":    strconv.Itoa(13),
		"num_storage": strconv.Itoa(14),
		"net_proto":   "rdma",
		"uuid":        fsUUID,
	}, "uuid")
	require.NoError(t, err)
	require.Equal(t, "capacity=1000&mac=zYkx0AC9zfyAVtW2QsJPGmETKIJSd_WY-P0690cph5Q&net_proto=rdma&num_meta=13&num_storage=14&uuid=019ba41c-a5d6-7d68-a1ec-d3c8c0f04018", getArgs)
}
