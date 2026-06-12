package main

// perf-test-subscriber measures watch event subscriber dispatch performance on a live system.
// It starts N concurrent gRPC subscriber servers on consecutive ports, receives events from a
// running beegfs-watch instance, and prints a throughput/reliability report after --duration.
//
// Typical usage:
//
//	perf-test-subscriber --num-subscribers 4 --duration 30s
//
// Each subscriber server must be registered in the beegfs-watch TOML config before this binary
// starts. The run-perf-test.sh script handles config generation and process lifecycle.

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/watch/pkg/subscriber"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

var (
	numSubscribers    = flag.Int("num-subscribers", 1, "number of concurrent subscriber servers to run")
	basePort          = flag.Int("base-port", 50051, "first port; each additional subscriber uses base-port+i")
	duration          = flag.Duration("duration", 60*time.Second, "how long to run before printing the report and exiting")
	ackFreq           = flag.Duration("ack-frequency", 1*time.Second, "how often to acknowledge events back to beegfs-watch (0 disables)")
	disableTLS        = flag.Bool("grpc-disable-tls", true, "disable TLS on subscriber gRPC servers")
	perfProfilingPort = flag.Int("perf-profiling-port", 0, "port to expose pprof on localhost (0 disables)")
	reportFormat      = flag.String("report-format", "text", "output format: text or csv")
	logDebug          = flag.Bool("log-debug", false, "enable debug logging")
)

type subStats struct {
	totalEvents   atomic.Uint64
	droppedEvents atomic.Uint64
	reconnects    atomic.Uint64
	lastSeqID     uint64 // accessed only by the per-subscriber drain goroutine
	firstSeen     bool
}

type serverEntry struct {
	server *subscriber.Server
	events chan *bw.Event
	acks   chan subscriber.Ack
	stats  *subStats
	port   int
}

type epsSample struct {
	ts     time.Time
	totals []uint64 // per-subscriber cumulative event count
}

func main() {
	flag.Parse()

	log, err := buildLogger(*logDebug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	if *perfProfilingPort != 0 {
		go func() {
			addr := fmt.Sprintf("localhost:%d", *perfProfilingPort)
			log.Info("pprof available", zap.String("address", addr))
			http.ListenAndServe(addr, nil)
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	servers := make([]serverEntry, *numSubscribers)
	var wg sync.WaitGroup

	for i := range *numSubscribers {
		port := *basePort + i
		events := make(chan *bw.Event, 1024)
		acks := make(chan subscriber.Ack, 64)

		srv, err := subscriber.NewServer(log, subscriber.Config{
			Address:      fmt.Sprintf(":%d", port),
			TlsDisable:   *disableTLS,
			AckFrequency: *ackFreq,
		}, subscriber.NoopCheckpointer{})
		if err != nil {
			log.Fatal("failed to create subscriber server", zap.Int("port", port), zap.Error(err))
		}

		errCh := make(chan error, 1)
		srv.ListenAndServe(events, acks, errCh)

		go func() {
			if err := <-errCh; err != nil {
				log.Error("subscriber server error", zap.Int("port", port), zap.Error(err))
			}
		}()

		servers[i] = serverEntry{
			server: srv,
			events: events,
			acks:   acks,
			stats:  &subStats{},
			port:   port,
		}
	}

	log.Info("all subscriber servers started", zap.Int("count", *numSubscribers), zap.Int("basePort", *basePort))

	for i := range servers {
		e := &servers[i]
		wg.Go(func() {
			drainEvents(ctx, log, e.events, e.acks, e.stats, e.port)
			close(e.acks)
		})
	}

	samples := make([]epsSample, 0, int(duration.Seconds())+2)
	sampleTicker := time.NewTicker(time.Second)
	defer sampleTicker.Stop()
	deadline := time.NewTimer(*duration)
	defer deadline.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-deadline.C:
			break loop
		case t := <-sampleTicker.C:
			snap := epsSample{ts: t, totals: make([]uint64, len(servers))}
			for i := range servers {
				snap.totals[i] = servers[i].stats.totalEvents.Load()
			}
			samples = append(samples, snap)
		}
	}

	for i := range servers {
		servers[i].server.Stop()
		close(servers[i].events) // unblock drainEvents; safe because Stop() waits for all ReceiveEvents handlers to exit
	}
	wg.Wait()

	printReport(servers, samples)
}

func drainEvents(ctx context.Context, log *zap.Logger, events <-chan *bw.Event, acks chan<- subscriber.Ack, s *subStats, port int) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}

			if s.firstSeen {
				if event.SeqId > s.lastSeqID+1 {
					dropped := event.SeqId - s.lastSeqID - 1
					s.droppedEvents.Add(dropped)
					log.Warn("detected dropped events",
						zap.Int("port", port),
						zap.Uint64("expected", s.lastSeqID+1),
						zap.Uint64("got", event.SeqId),
						zap.Uint64("dropped", dropped),
					)
				} else if event.SeqId < s.lastSeqID {
					// SeqId going backwards signals a Watch reconnect to a different metadata
					// server or a cursor reset; count it but don't treat as a drop.
					s.reconnects.Add(1)
				}
			} else {
				s.firstSeen = true
			}

			s.lastSeqID = event.SeqId
			s.totalEvents.Add(1)

			acks <- subscriber.Ack{MetaId: event.MetaId, SeqId: event.SeqId}
		}
	}
}

func printReport(servers []serverEntry, samples []epsSample) {
	type row struct {
		port          int
		totalEvents   uint64
		droppedEvents uint64
		reconnects    uint64
		avgEPS        float64
		peakEPS       float64
	}

	rows := make([]row, len(servers))
	for i, e := range servers {
		rows[i] = row{
			port:          e.port,
			totalEvents:   e.stats.totalEvents.Load(),
			droppedEvents: e.stats.droppedEvents.Load(),
			reconnects:    e.stats.reconnects.Load(),
		}
	}

	intervals := float64(len(samples) - 1)
	if intervals < 1 {
		intervals = 1
	}

	for i := 1; i < len(samples); i++ {
		dt := samples[i].ts.Sub(samples[i-1].ts).Seconds()
		if dt <= 0 {
			continue
		}
		for j := range rows {
			eps := float64(samples[i].totals[j]-samples[i-1].totals[j]) / dt
			rows[j].avgEPS += eps
			if eps > rows[j].peakEPS {
				rows[j].peakEPS = eps
			}
		}
	}

	var aggTotal, aggDropped, aggReconnects uint64
	var aggAvgEPS, aggPeakEPS float64
	for i := range rows {
		rows[i].avgEPS /= intervals
		aggTotal += rows[i].totalEvents
		aggDropped += rows[i].droppedEvents
		aggReconnects += rows[i].reconnects
		aggAvgEPS += rows[i].avgEPS
		if rows[i].peakEPS > aggPeakEPS {
			aggPeakEPS = rows[i].peakEPS
		}
	}

	if *reportFormat == "csv" {
		fmt.Println("port,total_events,dropped_events,reconnects,avg_eps,peak_eps")
		for _, r := range rows {
			fmt.Printf("%d,%d,%d,%d,%.1f,%.1f\n",
				r.port, r.totalEvents, r.droppedEvents, r.reconnects, r.avgEPS, r.peakEPS)
		}
		fmt.Printf("aggregate,%d,%d,%d,%.1f,%.1f\n",
			aggTotal, aggDropped, aggReconnects, aggAvgEPS, aggPeakEPS)
		return
	}

	fmt.Println()
	fmt.Printf("%-10s  %14s  %14s  %10s  %10s  %10s\n",
		"Port", "TotalEvents", "DroppedEvents", "Reconnects", "AvgEPS", "PeakEPS")
	fmt.Printf("%-10s  %14s  %14s  %10s  %10s  %10s\n",
		"----", "-----------", "-------------", "----------", "------", "-------")
	for _, r := range rows {
		fmt.Printf("%-10d  %14d  %14d  %10d  %10.1f  %10.1f\n",
			r.port, r.totalEvents, r.droppedEvents, r.reconnects, r.avgEPS, r.peakEPS)
	}
	fmt.Printf("%-10s  %14d  %14d  %10d  %10.1f  %10.1f\n",
		"aggregate", aggTotal, aggDropped, aggReconnects, aggAvgEPS, aggPeakEPS)
	fmt.Println()
}

func buildLogger(debug bool) (*zap.Logger, error) {
	if debug {
		return zap.NewDevelopment()
	}
	return zap.NewProduction()
}
