package index

type FindRow struct {
	Path  string
	Name  string
	Type  string
	Inode string
	Size  string
	Mtime string
	Atime string
	Ctime string
	Mode  string
	Uid   string
	Gid   string
	Nlink string

	OwnerID          string
	ParentEntryID    string
	EntryID          string
	StripeNumTargets string

	TargetOrGroup string
}

func ParseFindRow(raw []string, beegfs, targets bool) FindRow {
	r := FindRow{
		Path:  colAt(raw, 0),
		Name:  colAt(raw, 1),
		Type:  colAt(raw, 2),
		Inode: colAt(raw, 3),
		Size:  colAt(raw, 4),
		Mtime: colAt(raw, 5),
		Atime: colAt(raw, 6),
		Ctime: colAt(raw, 7),
		Mode:  colAt(raw, 8),
		Uid:   colAt(raw, 9),
		Gid:   colAt(raw, 10),
		Nlink: colAt(raw, 11),
	}
	// Both --targets (FindTargetsE) and --beegfs (FindBeeGFSE) select the four
	// BeeGFS columns at 12-15; --targets adds target_or_group at 16.
	if targets || beegfs {
		r.OwnerID = colAt(raw, 12)
		r.ParentEntryID = colAt(raw, 13)
		r.EntryID = colAt(raw, 14)
		r.StripeNumTargets = colAt(raw, 15)
	}
	if targets {
		r.TargetOrGroup = colAt(raw, 16)
	}
	return r
}

// StatRow holds one parsed result row of StatCoreE / StatBeeGFSE.
type StatRow struct {
	Name   string
	Type   string
	Inode  string
	Size   string
	Blocks string
	Mode   string
	Uid    string
	Gid    string
	Nlink  string
	Atime  string
	Mtime  string
	Ctime  string

	OwnerID           string
	ParentEntryID     string
	EntryID           string
	StripePatternType string
	StripeChunkSize   string
	StripeNumTargets  string
}

// ParseStatRow maps a raw stat result row onto StatRow. Column order must
// match StatCoreE / StatBeeGFSE.
func ParseStatRow(raw []string, beegfs bool) StatRow {
	r := StatRow{
		Name:   colAt(raw, 0),
		Type:   colAt(raw, 1),
		Inode:  colAt(raw, 2),
		Size:   colAt(raw, 3),
		Blocks: colAt(raw, 4),
		Mode:   colAt(raw, 5),
		Uid:    colAt(raw, 6),
		Gid:    colAt(raw, 7),
		Nlink:  colAt(raw, 8),
		Atime:  colAt(raw, 9),
		Mtime:  colAt(raw, 10),
		Ctime:  colAt(raw, 11),
	}
	if beegfs {
		r.OwnerID = colAt(raw, 12)
		r.ParentEntryID = colAt(raw, 13)
		r.EntryID = colAt(raw, 14)
		r.StripePatternType = colAt(raw, 15)
		r.StripeChunkSize = colAt(raw, 16)
		r.StripeNumTargets = colAt(raw, 17)
	}
	return r
}

type LsRow struct {
	Path   string
	Name   string
	Type   string
	Inode  string
	Size   string
	Mtime  string
	Atime  string
	Ctime  string
	Mode   string
	Uid    string
	Gid    string
	Nlink  string
	Blocks string

	OwnerID           string
	ParentEntryID     string
	EntryID           string
	StripePatternType string
	StripeChunkSize   string
	StripeNumTargets  string
}

func ParseLsRow(raw []string, recursive, beegfs bool) LsRow {
	off := 0
	var r LsRow
	if recursive {
		r.Path = colAt(raw, off)
		off++
	}
	r.Name = colAt(raw, off)
	off++
	r.Type = colAt(raw, off)
	off++
	r.Inode = colAt(raw, off)
	off++
	r.Size = colAt(raw, off)
	off++
	r.Mtime = colAt(raw, off)
	off++
	r.Atime = colAt(raw, off)
	off++
	r.Ctime = colAt(raw, off)
	off++
	r.Mode = colAt(raw, off)
	off++
	r.Uid = colAt(raw, off)
	off++
	r.Gid = colAt(raw, off)
	off++
	r.Nlink = colAt(raw, off)
	off++
	r.Blocks = colAt(raw, off)
	off++

	if beegfs {
		r.OwnerID = colAt(raw, off+0)
		r.ParentEntryID = colAt(raw, off+1)
		r.EntryID = colAt(raw, off+2)
		r.StripePatternType = colAt(raw, off+3)
		r.StripeChunkSize = colAt(raw, off+4)
		r.StripeNumTargets = colAt(raw, off+5)
	}
	return r
}
