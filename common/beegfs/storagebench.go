package beegfs

// This file contains common types related to working with BeeGFS storage bench.

// StorageBenchAction is the user requested action for the benchmark mode.
type StorageBenchAction int32

const (
	BenchUnspecified StorageBenchAction = -1
	BenchStart       StorageBenchAction = 0
	BenchStop        StorageBenchAction = 1
	BenchStatus      StorageBenchAction = 2
	BenchCleanup     StorageBenchAction = 3
)

func (a StorageBenchAction) String() string {
	switch a {
	case BenchStart:
		return "start"
	case BenchStop:
		return "stop"
	case BenchStatus:
		return "status"
	case BenchCleanup:
		return "cleanup"
	default:
		return "unspecified"
	}
}

// StorageBenchType is the type of benchmark test that was requested.
type StorageBenchType int32

const (
	NoBench    StorageBenchType = -1
	ReadBench  StorageBenchType = 0
	WriteBench StorageBenchType = 1
)

func (b StorageBenchType) String() string {
	switch b {
	case ReadBench:
		return "read"
	case WriteBench:
		return "write"
	default:
		return "unknown"
	}
}

// StorageBenchStatus is the status of the benchmark mode for a single storage node. See the
// StorageBenchError if the status is BenchError.
type StorageBenchStatus int32

const (
	BenchUninitialized StorageBenchStatus = 0
	BenchInitialized   StorageBenchStatus = 1
	BenchError         StorageBenchStatus = 2
	BenchRunning       StorageBenchStatus = 3
	BenchStopping      StorageBenchStatus = 4
	BenchStopped       StorageBenchStatus = 5
	BenchFinishing     StorageBenchStatus = 6
	BenchFinished      StorageBenchStatus = 7
)

func (s StorageBenchStatus) String() string {
	switch s {
	case BenchUninitialized:
		return "uninitialized"
	case BenchInitialized:
		return "initialized"
	case BenchError:
		return "error"
	case BenchRunning:
		return "running"
	case BenchStopping:
		return "stopping"
	case BenchStopped:
		return "stopped"
	case BenchFinishing:
		return "finishing"
	case BenchFinished:
		return "finished"
	default:
		return "unknown"
	}
}

// StorageBenchError communicates additional details if a node reports the BenchError status.
type StorageBenchError int32

const (
	// Communication and Worker Errors
	BenchErr_CommTimeout    StorageBenchError = -3
	BenchErr_AbortBenchmark StorageBenchError = -2
	BenchErr_WorkerError    StorageBenchError = -1
	BenchErr_NoError        StorageBenchError = 0
	BenchErr_Uninitialized  StorageBenchError = 1

	// Initialization Errors
	BenchErr_InitializationError   StorageBenchError = 10
	BenchErr_InitReadData          StorageBenchError = 11
	BenchErr_InitCreateBenchFolder StorageBenchError = 12
	BenchErr_InitTransferData      StorageBenchError = 13

	// Runtime Errors
	BenchErr_RuntimeError            StorageBenchError = 20
	BenchErr_RuntimeDeleteFolder     StorageBenchError = 21
	BenchErr_RuntimeOpenFiles        StorageBenchError = 22
	BenchErr_RuntimeUnknownTarget    StorageBenchError = 23
	BenchErr_RuntimeIsRunning        StorageBenchError = 24
	BenchErr_RuntimeCleanupJobActive StorageBenchError = 25
)

func (s StorageBenchError) Error() string {
	return s.String()
}

func (s StorageBenchError) String() string {
	switch s {
	case BenchErr_CommTimeout:
		return "communication timeout"
	case BenchErr_AbortBenchmark:
		return "benchmark aborted"
	case BenchErr_WorkerError:
		return "I/O error from worker"
	case BenchErr_NoError:
		return "executed benchmark command (no error)"
	case BenchErr_Uninitialized:
		return "benchmark uninitialized"

	// Initialization Errors
	case BenchErr_InitializationError:
		return "initialization error"
	case BenchErr_InitReadData:
		return "insufficient data available for read benchmark"
	case BenchErr_InitCreateBenchFolder:
		return "unable to create benchmark folder"
	case BenchErr_InitTransferData:
		return "unable to initialize random data"

	// Runtime Errors
	case BenchErr_RuntimeError:
		return "runtime error"
	case BenchErr_RuntimeDeleteFolder:
		return "unable to delete benchmark files"
	case BenchErr_RuntimeOpenFiles:
		return "unable to open benchmark file"
	case BenchErr_RuntimeUnknownTarget:
		return "specified target ID is unknown"
	case BenchErr_RuntimeIsRunning:
		return "unable to start a new benchmark because a benchmark is already running"
	case BenchErr_RuntimeCleanupJobActive:
		return "unable to cleanup benchmark folder because a benchmark is still in progress"

	default:
		return "unknown benchmark error"
	}
}
