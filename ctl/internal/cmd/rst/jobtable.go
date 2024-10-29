package rst

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/v8/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// jobsTable is an opinionated TableWrapper used to simplify and consistently print details about
// jobs, work requests, and work results using a table layout. It automatically handles wrapping
// select fields known to carry long text strings into a reasonable multi-line text strings that
// fits neatly into the table format.
type jobsTable struct {
	wrappingWidth int
	tbl           cmdfmt.TableWrapper
}

type jobTableCfg struct {
	defaultJobColumns []string
	columnWidth       int
	jobDetails        bool
}
type jobTableOpt func(*jobTableCfg)

func withDefaultColumns(defaultJobFields []string) jobTableOpt {
	return func(cfg *jobTableCfg) {
		cfg.defaultJobColumns = defaultJobFields
	}
}

func withJobDetails(enable bool) jobTableOpt {
	return func(cfg *jobTableCfg) {
		cfg.jobDetails = enable
	}
}

func withColumnWidth(width int) jobTableOpt {
	return func(cfg *jobTableCfg) {
		cfg.columnWidth = width
	}
}

// Creates a new table for printing jobs. If the global debug flag is set automatically prints all
// columns. If withJobDetails is set as an option it automatically prints all columns related to the
// job. A different set of default columns can be specified withDefaultColumns. Adjust the max width
// of some columns with withColumnWidth.
func newJobsTable(opts ...jobTableOpt) jobsTable {

	cfg := &jobTableCfg{
		// The default set of fields to print for a job. The last unlabeled column is used when
		// printing out a minimal row, so errors that should not be hidden can be displayed. These
		// types of errors are not put in the status column because (a) that is technically
		// inaccurate and (b) it would require always displaying the status message column, which
		// often has overflow and means table rows will span multiple lines more often than needed.
		defaultJobColumns: []string{"ok", "path", "target", "last update", "request type", ""},
		columnWidth:       35,
		jobDetails:        false,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// IMPORTANT: Use caution when updating column names and ensure to also update the column names
	// anywhere that has overridden the default columns with withDefaultColumns.
	//
	// The full set of fields related to a particular job, not including work requests and results.
	allJobColumns := []string{"ok", "path", "target", "created", "last update", "job id", "request type", "state", "status message", ""}
	// All fields for this job and its work requests and results. Information for a particular job
	// will generally need to be printed using multiple rows.
	allJobAndWorkColumns := []string{"ok", "path", "target", "created", "last update", "job id", "request type", "state", "status message", "work requests", "work results", ""}

	if viper.GetBool(config.DebugKey) {
		cfg.defaultJobColumns = allJobAndWorkColumns
	} else if cfg.jobDetails {
		cfg.defaultJobColumns = allJobColumns
	}
	return jobsTable{
		wrappingWidth: cfg.columnWidth,
		tbl:           cmdfmt.NewTableWrapper(allJobAndWorkColumns, cfg.defaultJobColumns),
	}
}

func (t *jobsTable) PrintRemaining() {
	t.tbl.PrintRemaining()
}

// Row adds a row for the provided job. It is opinionated about what fields are included, and how
// each field is displayed.
func (t *jobsTable) Row(job *beeremote.JobResult) {
	var operation string
	// Must be updated when new job types are supported:
	if job.Job.GetRequest().GetSync() != nil {
		operation = job.Job.GetRequest().GetSync().Operation.String()
	} else {
		// Fallback and print the raw representation for unknown types:
		operation = fmt.Sprintf("%v", job.Job.GetRequest().GetType())
	}

	t.tbl.Row(
		convertJobStateToEmoji(job.Job.GetStatus().GetState()),
		job.Job.GetRequest().GetPath(),
		job.Job.GetRequest().GetRemoteStorageTarget(),
		job.Job.GetCreated().AsTime().Format(time.RFC3339),
		job.Job.GetStatus().GetUpdated().AsTime().Format(time.RFC3339),
		job.Job.GetId(),
		operation,
		job.Job.GetStatus().GetState(),
		wrapTextAtWidth(job.Job.GetStatus().GetMessage(), t.wrappingWidth),
		t.getWorkRequestsForCell(job.GetWorkRequests()),
		t.getWorkResultsForCell(job.GetWorkResults()),
		"",
		// When making updates, also update MinimalRow().
	)
}

// MinimalRow() adds a row for a job that could not be created for some reason. This is helpful when
// listing jobs that may or may not have been actually created. It only populates the path and
// status message field based on the error, putting a question mark/unknown in the ok field.
func (t *jobsTable) MinimalRow(path string, err error) {
	t.tbl.Row(
		convertJobStateToEmoji(beeremote.Job_UNKNOWN),
		path,
		"-",
		"-",
		"-",
		"-",
		"-",
		"-",
		"-",
		"-",
		"-",
		fmt.Sprintf("WARNING: %s", err.Error()),
	)
}

// getWorkRequestsForCell() takes a slice of work requests and condenses the output to fit nicely
// into a table cell. It is primarily intended to be used by Row().
func (t *jobsTable) getWorkRequestsForCell(workRequests []*flex.WorkRequest) string {
	strBuilder := strings.Builder{}
	for _, wr := range workRequests {
		strBuilder.WriteString(fmt.Sprintf("===== request id: %s =====\n* external id: \n%s\n* offset start: %d\n* offset stop: %d\n* part start: %d\n* part stop: %d\n",
			wr.RequestId,
			wrapTextAtWidth(wr.ExternalId, t.wrappingWidth),
			wr.Segment.OffsetStart,
			wr.Segment.OffsetStop,
			wr.Segment.PartsStart,
			wr.Segment.PartsStop,
		))
	}
	return strBuilder.String()
}

// getWorkResultsForCell() takes a slice of work results and condenses the output to fit nicely into
// a table cell. It is primarily intended to be used by Row().
func (t *jobsTable) getWorkResultsForCell(workResults []*beeremote.JobResult_WorkResult) string {
	strBuilder := strings.Builder{}
	for _, wr := range workResults {
		strBuilder.WriteString(fmt.Sprintf("===== request id: %s =====\n* status: %s\n* message: \n%s\n* part list:%s\n",
			wr.GetWork().GetRequestId(),
			wr.GetWork().GetStatus().GetState(),
			wrapTextAtWidth(wr.GetWork().GetStatus().Message, t.wrappingWidth),
			t.getPartsForCell(wr.GetWork().Parts),
		))
	}
	return strBuilder.String()
}

// getPartsForCell() takes a slice of parts and condenses the output to fit nicely into a table cell.
// It is primarily intended to be used by Row().
func (t *jobsTable) getPartsForCell(parts []*flex.Work_Part) string {
	strBuilder := strings.Builder{}
	for _, part := range parts {
		strBuilder.WriteString(fmt.Sprintf("\n * part: %d\n  * offset start: %d\n  * offset stop: %d\n  * entity tag: \n%s\n  * sha256sum: \n%s",
			part.GetPartNumber(),
			part.GetOffsetStart(),
			part.GetOffsetStop(),
			wrapTextAtWidth(part.GetEntityTag(), t.wrappingWidth),
			wrapTextAtWidth(part.GetChecksumSha256(), t.wrappingWidth),
		))

	}
	return strBuilder.String()
}

// wrapTextAtWidth is used to wrap long text such as checksums or entity IDs across multiple rows
// enclosed by pipe characters. It is primarily intended for use from the getXForCell() functions.
// The resulting output looks like:
//
//	| my lo |
//	| ng te |
//	| xt    |
func wrapTextAtWidth(text string, width int) string {
	strBuilder := strings.Builder{}
	if width <= 0 {
		return text
	}
	l := len(text)
	for i := 0; i < l; i += width {
		end := i + width
		if end > len(text) {
			end = len(text)
		}
		if i == 0 {
			// Handle printing the first line.
			if l == end {
				// If there is no overflow just print as is.
				strBuilder.WriteString(text[i:end] + strings.Repeat(" ", width-len(text[i:end])))
			} else {
				// Otherwise print the first line, subsequent lines handle printing the newline:
				strBuilder.WriteString("| " + text[i:end] + strings.Repeat(" ", width-len(text[i:end])) + " |")
			}
		} else if l == end {
			// Once we reach the end of the string pad the closing pipe so they all line up.
			strBuilder.WriteString("\n| " + text[i:end] + strings.Repeat(" ", width-len(text[i:end])) + " |")
		} else {
			// Print all lines other than the first or the last line.
			strBuilder.WriteString("\n| " + text[i:end] + " |")
		}
	}
	return strBuilder.String()
}

func convertJobStateToEmoji(state beeremote.Job_State) string {
	representation, exists := jobStateMap[state]
	if !exists {
		return "�"
	}
	if !viper.GetBool(config.DisableEmojisKey) {
		return representation.emoji
	}
	return representation.alternative
}

type jobStateEmoji struct {
	emoji       string
	alternative string
}

var jobStateMap = map[beeremote.Job_State]jobStateEmoji{
	beeremote.Job_UNKNOWN:    {"❓", beeremote.Job_UNKNOWN.String()},
	beeremote.Job_UNASSIGNED: {"⏳", beeremote.Job_UNASSIGNED.String()},
	beeremote.Job_SCHEDULED:  {"⏳", beeremote.Job_SCHEDULED.String()},
	beeremote.Job_RUNNING:    {"⏳", beeremote.Job_RUNNING.String()},
	beeremote.Job_ERROR:      {"\u26A0\ufe0f ", beeremote.Job_ERROR.String()},
	beeremote.Job_FAILED:     {"❌", beeremote.Job_FAILED.String()},
	beeremote.Job_CANCELLED:  {"🚫", beeremote.Job_CANCELLED.String()},
	beeremote.Job_COMPLETED:  {"✅", beeremote.Job_COMPLETED.String()},
}

func convertWorkStateToEmoji(state flex.Work_State) string {
	representation, exists := workStateMap[state]
	if !exists {
		return "�"
	}
	if !viper.GetBool(config.DisableEmojisKey) {
		return representation.emoji
	}
	return representation.alternative
}

type workStateEmoji struct {
	emoji       string
	alternative string
}

var workStateMap = map[flex.Work_State]workStateEmoji{
	flex.Work_UNKNOWN:   {"❓", flex.Work_UNKNOWN.String()},
	flex.Work_CREATED:   {"⏳", flex.Work_CREATED.String()},
	flex.Work_SCHEDULED: {"⏳", flex.Work_SCHEDULED.String()},
	flex.Work_RUNNING:   {"⏳", flex.Work_RUNNING.String()},
	flex.Work_ERROR:     {"\u26A0\ufe0f ", flex.Work_ERROR.String()},
	flex.Work_FAILED:    {"❌", flex.Work_FAILED.String()},
	flex.Work_CANCELLED: {"🚫", flex.Work_CANCELLED.String()},
	flex.Work_COMPLETED: {"✅", flex.Work_COMPLETED.String()},
}
