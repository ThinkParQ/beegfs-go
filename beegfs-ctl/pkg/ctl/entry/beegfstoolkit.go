package entry

// Go implementations of various BeeGFS toolkit functions required to print verbose entry output.

import (
	"fmt"
	"strings"
)

const (
	// STORAGETK_TIMESTAMP_DAY_RPOS in the BeeGFS StorageTK. Note RPos means reverse position (i.e.,
	// the position is from the end of the string).
	timestampDayRPos = 3
	// META_DENTRIES_LEVEL1_SUBDIR_NUM in the MetaStorageTk.
	metaDentriesLevel1SubDirNum = 128
	// META_DENTRIES_LEVEL2_SUBDIR_NUM in the MetaStorage TK.
	metaDentriesLevel2SubDirNum = 128
	// META_INODES_LEVEL1_SUBDIR_NUM in the MetaStorage TK.
	metaInodesLevel1SubDirNum = 128
	// META_INODES_LEVEL2_SUBDIR_NUM in the MetaStorage TK.
	metaInodesLevel2SubDirNum = 128
)

// getFileChunkPath() from the BeeGFS StorageTK. As with the original implementation, it will always
// attempt to return a chunkPath even if errors are encountered. The caller should check the error
// and decide if the chunkPath returned is still usable.
//
// IMPORTANT: This only works with the 2014.01 style path with user and timestamp dirs.
func getFileChunkPath(originalParentUID uint32, originalParentEntryID string, entryID string) (string, error) {
	uidStr := fmt.Sprintf("%X", originalParentUID)
	timestamp, err := timestampFromEntryID(string(originalParentEntryID))
	yearMonth, day := timestampToPath(timestamp)
	chunkPath := "u" + uidStr + "/" +
		yearMonth + "/" + // Level 1 - approx yymm
		day + "/" + // Level 2 - approx dd
		originalParentEntryID + "/" +
		entryID

	return chunkPath, err
}

// timestampFromEntryID() from the BeeGFS StringTK.
func timestampFromEntryID(entryID string) (string, error) {
	splitParentEntryID := strings.Split(entryID, "-")
	if l := len(splitParentEntryID); l == 1 {
		// For special entries like 'root' and 'lost+found' just return as is.
		return entryID, nil
	} else if l != 3 {
		// This shouldn't happen, just return the entry as is but the caller should log an error.
		return entryID, fmt.Errorf("unable to parse timestamp from original parent entry ID %s", entryID)
	}
	return splitParentEntryID[1], nil
}

// timeStampToPath() from the BeeGFS StorageTK.
func timestampToPath(timestamp string) (yearMonth string, day string) {
	tsLastPos := len(timestamp) - 1
	tsDayPos := tsLastPos - timestampDayRPos

	if tsDayPos < 0 {
		day = "0"
	} else {
		day = timestamp[tsLastPos-timestampDayRPos : tsLastPos-timestampDayRPos+1]
	}

	if tsDayPos == 0 {
		yearMonth = "0"
	} else {
		yearMonth = timestamp[:tsDayPos]
	}
	return yearMonth, day
}

// getMetaDirEntryPath() from the BeeGFS MetaStorageTk.
func getMetaDirEntryPath(dentriesPath string, dirEntryID string) string {
	return getHashPath(dentriesPath, dirEntryID, metaDentriesLevel1SubDirNum, metaDentriesLevel2SubDirNum)
}

// getMetaInodePath() in the BeeGFS MetaStorageTk.
func getMetaInodePath(inodePath string, entryID string) string {
	return getHashPath(inodePath, entryID, metaInodesLevel1SubDirNum, metaInodesLevel2SubDirNum)
}

// getHashPath() in the BeeGFS StorageTk.
func getHashPath(path string, entryID string, numHashesLevel1 int, numHashesLevel2 int) string {
	return path + "/" + getBaseHashPath(entryID, numHashesLevel1, numHashesLevel2)
}

// getBaseHashPath() in the BeeGFS StorageTk.
func getBaseHashPath(entryID string, numHashLevel1 int, numHashLevel2 int) string {
	l1, l2 := getHashes(entryID, numHashLevel1, numHashLevel2)
	return fmt.Sprintf("%X/%X/%s", l1, l2, entryID)
}

// getHashes() in the BeeGFS StorageTk.
func getHashes(hashStr string, numHashesLevel1 int, numHashesLevel2 int) (uint16, uint16) {
	checksum := hash32(hashStr)
	// The checksum is a uint32, so to consume all bits we:
	//  - Take the upper 16 bits as a uint16 outHashLevel1.
	//  - Take the lower 16 bits as a uint16 outHashLevel2.
	outHashLevel1 := uint16(checksum>>16) % uint16(numHashesLevel1)
	outHashLevel2 := uint16(checksum) % uint16(numHashesLevel2)
	return outHashLevel1, outHashLevel2
}
