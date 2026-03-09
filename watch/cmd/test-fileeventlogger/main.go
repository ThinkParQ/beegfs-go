package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
)

var (
	socketPath      = flag.String("socket", "/run/beegfs/eventlog", "The path to the BeeGFS event log socket")
	logFile         = flag.String("logFile", "", "log to a file instead of stdout")
	logDebug        = flag.Bool("logDebug", false, "enable logging at the debug level")
	frequency       = flag.Duration("frequency", 1*time.Second, "how often an event should be sent to the socket (0 is as fast as possible)")
	pathLengths     = flag.Int("pathLengths", 0, "the length to use for the srcPath and tgtPath (if the length is zero then random path lengths will be used)")
	numRandomEvents = flag.Int("numRandomEvents", 5, "if pathLengths is zero, this many randomly sized events will be generated and sent at random")
)

var log *zap.Logger

func main() {
	flag.Parse()

	var err error
	log, err = getLogger()
	if err != nil {
		fmt.Println("Unable to initialize logger: ", err)
		os.Exit(1)
	}

	// Setup the socket:
	conn, err := net.Dial("unixpacket", *socketPath)

	if err != nil {
		log.Fatal("error listening on socket", zap.Error(err))
	}

	defer conn.Close()

	events := [][]byte{}

	if *pathLengths == 0 {
		// Generate five random events:
		for i := 0; i <= *numRandomEvents; i++ {
			events = append(events, generateEvent(rand.Intn(4093)+4))
		}
	} else {
		events = append(events, generateEvent(*pathLengths))
	}

	if *frequency == 0 {
		for {
			if *pathLengths == 0 {
				// Get a random event:
				_, err = conn.Write(events[rand.Intn(*numRandomEvents+1)])
			} else {
				_, err = conn.Write(events[0])
			}

			if err != nil {
				log.Fatal("error writing to socket", zap.Error(err))
			}
		}
	} else {
		// We'll connect common OS signals to a context to cleanly shutdown even if we're waiting to send an event:
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		defer cancel()

		ticker := time.NewTicker(*frequency)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Info("graceful shutdown requested")
				return
			case <-ticker.C:
				if *pathLengths == 0 {
					// Get a random event:
					_, err = conn.Write(events[rand.Intn(*numRandomEvents+1)])
				} else {
					_, err = conn.Write(events[0])
				}
				if err != nil {
					log.Fatal("error writing to socket", zap.Error(err))
				}
			}
		}
	}
}

// getLogger parses command line logging options and returns an appropriately configured zap.Logger.
func getLogger() (*zap.Logger, error) {

	var config zap.Config
	config.InitialFields = map[string]any{"serviceName": "bee-watch"}

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

func joinSlices(slices ...[]byte) []byte {
	var result []byte
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}

func generateEvent(pathLengths int) []byte {
	majorVersion := []byte{0x1, 0x0}
	minorVersion := []byte{0x0, 0x0}
	// Size must actually be larger than the full event packet.
	size := new(bytes.Buffer)
	err := binary.Write(size, binary.LittleEndian, 91+uint32(pathLengths)*2)
	if err != nil {
		log.Fatal("unable to generate size", zap.Error(err))
	}
	log.Info("generated event size", zap.Uint32("size", 91+uint32(pathLengths)*2))

	droppedSeq := []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	missedSeq := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	eventType := []byte{0xb, 0x0, 0x0, 0x0}
	entryIdLength := []byte{0xc, 0x0, 0x0, 0x0}
	entryID := []byte{0x30, 0x2d, 0x36, 0x34, 0x34, 0x42, 0x46, 0x46, 0x31, 0x46, 0x2d, 0x31, 0x0}
	parentEntryIDLength := []byte{0xc, 0x0, 0x0, 0x0}
	parentEntryID := []byte{0x30, 0x2d, 0x36, 0x34, 0x34, 0x43, 0x30, 0x30, 0x31, 0x42, 0x2d, 0x31, 0x0}
	// Allow for testing with variable sizes for src and tgt paths:
	pathLength := new(bytes.Buffer)
	err = binary.Write(pathLength, binary.LittleEndian, uint32(pathLengths))
	if err != nil {
		log.Fatal("unable to generate path length", zap.Error(err))
	}
	path := []byte(strings.Repeat("a", pathLengths) + "\x00")
	targetPathLength := pathLength // Unnecessary but done for clarity and in case we want to change this in the future.
	targetPath := []byte(strings.Repeat("b", pathLengths) + "\x00")
	targetParentIDLength := []byte{0x4, 0x0, 0x0, 0x0}
	targetParentID := []byte{0x72, 0x6f, 0x6f, 0x74, 0x0}

	return joinSlices(majorVersion, minorVersion, size.Bytes(), droppedSeq, missedSeq, eventType,
		entryIdLength, entryID, parentEntryIDLength, parentEntryID, pathLength.Bytes(), path,
		targetPathLength.Bytes(), targetPath, targetParentIDLength, targetParentID,
		[]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}) // Add extra bytes at the end because the meta service does this.
}
