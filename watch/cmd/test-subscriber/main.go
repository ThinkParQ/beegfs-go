package main

// ===== README FIRST: IMPLEMENTING YOUR OWN GRPC SUBSCRIBER ===== This demo
// implements a working example of how to read from the gRPC based event stream
// and write the events to a database. This file has comments "Step 1", "Step
// 2", ... that are intended to walk through defining your own gRPC subscriber
// that reads events from BeeWatch. These steps are intended to help the reader
// differentiate between what is actually needed to implement their own gRPC
// subscriber, and what is used to create a working demo.
//
// Note the steps start at step 0 and setting up your imports before walking
// through the implementation then actual use in the main function. This seemed
// like the logical order to present everything, but you could also start in the
// main function if you want to start by seeing the high level use then refer
// back to the implementations for additional details.
//
// IMPORTANT: gRPC operates around a client->server model. Here we are actually
// building the server-side of this relationship by implementing a gRPC server
// that accepts a stream of messages containing file system events from a gRPC
// client (BeeWatch).
//
// See the Advanced section of the project README for more details on how
// BeeWatch uses gRPC and protocol buffers along with best practices to follow
// when implementing your own subscriber.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"

	// Step 0: Import the precompiled BeeWatch Protocol Buffers and gRPC Service
	// Definitions. Refer to the documentation at
	// https://github.com/ThinkParQ/protobuf/README.md#quick-start for how to get
	// started.
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Define our flags and any other global variables needed for the demo to work.
var (
	logFile              = flag.String("log-file", "", "log to a file instead of stdout")
	logDebug             = flag.Bool("log-debug", false, "enable logging at the debug level")
	logIncomingEventRate = flag.Bool("log-incoming-event-rate", false, "output events per second")
	perfProfilingPort    = flag.Int("perf-profiling-port", 0, "specify a port where performance profiles will be made available on the localhost")
	grpcAddress          = flag.String("grpc-address", "localhost:50051", "What hostname:port this subscriber will use to listen for events from BeeWatch nodes.")
	ackFrequency         = flag.Duration("ack-frequency", 1*time.Second, "how often to acknowledge events back to BeeWatch (0 disables sending acks)")
	mockDBFilename       = flag.String("mock-db-filename", "scratch", "where store sequence IDs to allow the app to be restarted and detect dropped events")
	tlsCertificate       = flag.String("tls-cert-file", "/etc/beegfs/cert.pem", "Path to a certificate file that provides the identify of the subscriber gRPC server.")
	tlsKey               = flag.String("tls-key-file", "/etc/beegfs/key.pem", "Path to a key file belonging to the certificate for the subscriber gRPC server.")
	db                   = &MockDB{}
)

// Step 1: Create an EventSubscriberServer struct. This must embed the
// UnimplementedSubscriberServer along with anything else you want to use to
// coordinate the server. For example a logger and a waitgroup to coordinate
// shutdown.
type EventSubscriberServer struct {
	bw.UnimplementedSubscriberServer
	log *zap.Logger
	wg  sync.WaitGroup
}

// Helper function to initialize a new EventSubscriberServer.
func NewEventSubscriberServer(log *zap.Logger) *EventSubscriberServer {
	return &EventSubscriberServer{log: log}
}

// This check verified the EventSubscriberServer implements the SubscriberServer interface.
var _ bw.SubscriberServer = &EventSubscriberServer{}

// STEP 2: Implement the SubscriberServer interface.
// Currently only the ReceiveEvents method is required. This method will be
// called every time there is a new connection from a SubscriberClient. The
// client side will use the provided stream to send one or more messages (i.e.,
// events). The server will use the stream to send one or more responses.
// Sending and receiving on the stream can happen in parallel and servers can
// send responses before clients send messages.
func (s *EventSubscriberServer) ReceiveEvents(stream bw.Subscriber_ReceiveEventsServer) error {

	// Because there may be multiple connections at once we add each connection
	// to a wait group. When shutting down we'll make sure the app waits until
	// all connections are finished before exiting. This is important so we can
	// finish reading events from the connection then writing them to the
	// database.
	s.wg.Add(1)
	defer s.wg.Done()

	// This context will be cancelled when the server is stopped.
	// We'll use it to know when to shutdown our end of the stream.
	ctx := stream.Context()

	metaID, err := metaIDOf(ctx)
	if err != nil {
		s.log.Error("rejecting event stream", zap.Error(err))
		return err
	}
	s.log.Info("gRPC client connected", zap.Uint32("metaID", metaID))

	// Responding to every event is very inefficient. For this reason BeeWatch
	// intentionally allows us to selectively ack certain events. So we'll send
	// periodic responses in a separate goroutine. We make this time based (not
	// ever N seqIDs) because we want to ensure all events are eventually
	// acknowledged even if we stop receiving new events.
	if *ackFrequency != 0 {
		s.wg.Go(func() {
			sendAck := func(seqID uint64) {
				if err := stream.Send(&bw.Response{CompletedSeq: seqID}); err != nil {
					s.log.Error("error sending response", zap.Error(err), zap.Any("event", seqID))
				}
			}
			initialResponse, _ := db.GetSeqID(metaID)
			sendAck(initialResponse)
			ticker := time.NewTicker(*ackFrequency)
			for {
				select {
				case <-ctx.Done():
					s.log.Info("no longer sending responses because the stream was cancelled")
					return
				case <-ticker.C:
					if lastSeqID, stored := db.GetSeqID(metaID); stored {
						sendAck(lastSeqID)
					}
				}
			}
		})
	}
	// Listen for new events:
	for {
		select {
		case <-ctx.Done():
			s.log.Info("stream was cancelled (is the server shutting down?)")
			return ctx.Err()
		default:
		}

		// This will block until we receive a message or the stream's context is cancelled.
		// This is why we use genericGRPCServer.Stop() when shutting down. It will cancel
		// the event stream for us, then we handle the error so we know what to do next.
		event, err := stream.Recv()

		if err == io.EOF {
			s.log.Info("client closed the stream")
			return nil
		} else if err != nil {
			status, ok := status.FromError(err)
			if ok && status.Code() == codes.Canceled && status.Message() == "context canceled" {
				continue // Only log if the context wasn't cancelled.
			} else {
				s.log.Info("error receiving from client", zap.Error(err))
				continue
			}
		}

		s.log.Debug("received event", zap.Any("event", event))
		db.Add(metaID, event)
	}
}

func metaIDOf(ctx context.Context) (uint32, error) {
	nodeIDs := metadata.ValueFromIncomingContext(ctx, "node-id")
	if len(nodeIDs) == 0 {
		return 0, fmt.Errorf("event stream from watch does not include the metadata node-id")
	}
	entityID, err := beegfs.NewEntityIdParser(32, beegfs.Meta).Parse(nodeIDs[0])
	if err != nil {
		return 0, fmt.Errorf("unable to parse the metadata node-id %q: %w", nodeIDs[0], err)
	}
	legacyID, ok := entityID.(beegfs.LegacyId)
	if !ok {
		return 0, fmt.Errorf("the metadata node-id %q is not a numeric ID", nodeIDs[0])
	}
	return uint32(legacyID.NumId), nil
}

func main() {
	// Wire up flags and our logger.
	flag.Parse()
	log, err := getLogger()
	if err != nil {
		fmt.Println("Unable to initialize logger: ", err)
		os.Exit(1)
	}
	defer log.Sync() // Make sure we flush logs before shutting down.

	// Start the pprof server in the background if requested.
	// This works because of the `import _ "net/http/pprof"`.
	if *perfProfilingPort != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", *perfProfilingPort), nil)
		}()
	}

	// We'll connect common OS signals to a context to cleanly shutdown goroutines:
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Simulate a database that would be used to persist events to disk:
	var waitDB sync.WaitGroup
	dbCtx, dbShutdown := context.WithCancel(context.Background())
	db = newDB(dbCtx, log)
	go db.Run(&waitDB)
	waitDB.Add(1)

	// STEP 3: Identify a local network address that will be used to listen for
	// gRPC event streams:
	lis, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		log.Fatal("failed to setup listener for BeeWatch events", zap.Error(err))
	}

	// STEP 4: Define any general options needed for the gRPC server.
	// Use these options to create a generic gRPC server:
	var grpcServerOpts []grpc.ServerOption
	if *tlsCertificate != "" && *tlsKey != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCertificate, *tlsKey)
		if err != nil {
			log.Fatal("unable to setup TLS", zap.Error(err))
		}
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(creds))
	} else {
		log.Warn("not using TLS because -tls-certificate and/or -tls-key were not specified")
	}

	genericGRPCServer := grpc.NewServer(grpcServerOpts...)

	// STEP 5: Initialize a new instance of our EventSubscriberServer. Then
	// register the gRPC service and our concrete type providing the service
	// implementation so we can use the gRPC server to serve calls to ReceiveEvents:
	eventSubscriberServer := NewEventSubscriberServer(log)
	bw.RegisterSubscriberServer(genericGRPCServer, eventSubscriberServer)

	// Step 6: Run the gRPC server in a separate goroutine so we can catch
	// signals and coordinate shutdown in the main goroutine:
	go func() {
		log.Info("starting gRPC server")
		if genericGRPCServer.Serve(lis); err != nil {
			log.Fatal("unable to serve gRPC requests", zap.Error(err))
		}
	}()

	// Step 7: Wait here until we're signaled to shutdown:
	<-ctx.Done()

	// Step 8: Coordinate shutdown when a signal is received. First stop the gRPC
	// server then wait until all the methods handling individual client streams
	// are finished before shutting down the database.
	log.Info("shutting down gRPC server")
	genericGRPCServer.Stop()
	eventSubscriberServer.wg.Wait()
	// Why do we use Stop() and not GracefulStop()?
	// The grpcServer.GracefulStop() method will stop accepting new RPCs, but it
	// will not cancel any that are still in progress. This means we'll block at
	// stream.Recv() until the client disconnects, which is not how we want
	// things to work. We want the server to be able to disconnect at anytime,
	// and the client (BeeWatch) will buffer events for us.
	//
	// If we wanted to use GracefulStop() we would need ReceiveEvents() to start
	// stream.Recv() in a separate goroutine. That way ReceiveEvents() can watch
	// for the context to be cancelled and return causing Recv() to return an
	// error. The trouble is this can lead to race conditions that often cause
	// an event to be dropped. Using Stop() is less convoluted and lets us
	// always test the worst case scenario where streams don't close nicely.

	// NOTE: The following code is not needed to create a gRPC subscriber.
	log.Info("shutting down database")
	dbShutdown()
	waitDB.Wait()
	log.Info("all components stopped, exiting")
}

// NOTE: The following code is not needed to create a gRPC subscriber.
type MockDB struct {
	ctx    context.Context
	log    *zap.Logger
	events chan metaEvent
	mu     sync.Mutex
	seqIDs map[uint32]metaSeqIDs
}

type metaEvent struct {
	metaID uint32
	event  *bw.Event
}

type metaSeqIDs struct {
	lastSeqID      uint64
	lastDroppedSeq uint64
}

func newDB(ctx context.Context, log *zap.Logger) *MockDB {
	log = log.With(zap.String("component,", "database"))
	db := &MockDB{
		ctx:    ctx,
		log:    log,
		events: make(chan metaEvent),
		seqIDs: make(map[uint32]metaSeqIDs),
	}
	db.load()
	return db
}

func (db *MockDB) load() {
	data, err := os.ReadFile(*mockDBFilename)
	if errors.Is(err, os.ErrNotExist) {
		db.log.Info("resetting sequence IDs (file not found)", zap.Any("mockDBFilename", *mockDBFilename))
		return
	}
	if err != nil {
		db.log.Fatal("unable to read db file", zap.Error(err))
	}
	for line := range strings.Lines(string(data)) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var metaID uint32
		var seqIDs metaSeqIDs
		if _, err := fmt.Sscanf(line, "%d,%d,%d", &metaID, &seqIDs.lastSeqID, &seqIDs.lastDroppedSeq); err != nil {
			db.log.Fatal("unable to parse db file (each line must be meta,seq,dropped; delete the file to start over)", zap.String("line", line), zap.Error(err))
		}
		db.seqIDs[metaID] = seqIDs
		db.log.Info("using sequence IDs from file", zap.Uint32("metaID", metaID), zap.Any("lastSeqID", seqIDs.lastSeqID), zap.Any("lastDroppedSequence", seqIDs.lastDroppedSeq))
	}
}

func (db *MockDB) Sample() {

	for {
		select {
		case <-db.ctx.Done():
			return
		default:
			start := db.snapshot()
			startTime := time.Now()
			time.Sleep(time.Second)
			end := db.snapshot()
			duration := time.Since(startTime).Seconds()

			for metaID, seqIDs := range end {
				eventsReceived := seqIDs.lastSeqID - start[metaID].lastSeqID
				db.log.Info("events per second", zap.Uint32("metaID", metaID), zap.Any("EPS", float64(eventsReceived)/duration))
			}
		}
	}
}

func (db *MockDB) snapshot() map[uint32]metaSeqIDs {
	db.mu.Lock()
	defer db.mu.Unlock()
	return maps.Clone(db.seqIDs)
}

func (db *MockDB) Add(metaID uint32, event *bw.Event) {
	db.events <- metaEvent{metaID: metaID, event: event}
}

func (db *MockDB) GetSeqID(metaID uint32) (lastSeqID uint64, stored bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	seqIDs, stored := db.seqIDs[metaID]
	return seqIDs.lastSeqID, stored
}

func (db *MockDB) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	if *logIncomingEventRate {
		go db.Sample()
	}

readEvents:
	for {
		select {
		case <-db.ctx.Done():
			break readEvents
		case received := <-db.events:
			event := received.event
			lastSeqID, _ := db.GetSeqID(received.metaID)

			if event.SeqId != lastSeqID+1 && lastSeqID != 0 {
				if event.SeqId > lastSeqID+1 {
					db.log.Error("warning: client dropped event(s) or sent events out of order", zap.Uint32("metaID", received.metaID), zap.Any("expectedSeqID", lastSeqID+1), zap.Any("actualSeqID", event.SeqId))
				} else {
					db.log.Info("detected duplicate event", zap.Uint32("metaID", received.metaID), zap.Any("expectedSeqID", lastSeqID+1), zap.Any("actualSeqID", event.SeqId))
					// TODO: Think about how to handle this.
					// If we just reset the lastSeqID then we don't know about any additional duplicates.
					// If we just continue, we can have problems if the uint64 sequence ID rolls over.
					// Or if for some other reason BeeWatch and subscriber sequence IDs get out of sync.
					//continue readEvents
				}
			}

			db.mu.Lock()
			seqIDs := db.seqIDs[received.metaID]
			seqIDs.lastSeqID = event.SeqId
			db.seqIDs[received.metaID] = seqIDs
			db.mu.Unlock()
		}
	}

	seqIDs := db.snapshot()
	db.log.Info("writing out sequence IDs and shutting down", zap.Any("seqIDsByMeta", seqIDs))

	file, err := os.OpenFile(*mockDBFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		db.log.Error("unable to open file to write out sequence ID", zap.Error(err))
		return
	}

	var lines strings.Builder
	for _, metaID := range slices.Sorted(maps.Keys(seqIDs)) {
		fmt.Fprintf(&lines, "%d,%d,%d\n", metaID, seqIDs[metaID].lastSeqID, seqIDs[metaID].lastDroppedSeq)
	}
	_, err = file.WriteString(lines.String())
	if err != nil {
		db.log.Error("error writing out updated sequence IDs", zap.Error(err))
	}

	err = file.Sync()
	if err != nil {
		db.log.Error("error syncing database file", zap.Error(err))
	}

	db.log.Info("synchronized database to disk")
	if err := file.Close(); err != nil {
		db.log.Error("error closing db file: %s", zap.Error(err))
	}
}

// getLogger parses command line logging options and returns an appropriately configured zap.Logger.
func getLogger() (*zap.Logger, error) {

	var config zap.Config

	if *logDebug {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}

	if *logFile != "" {
		logFile, err := os.OpenFile(*logFile, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Println("unable to create log file: ", err)
			os.Exit(1)
		}

		config.OutputPaths = []string{logFile.Name()}
	}

	return config.Build()
}
