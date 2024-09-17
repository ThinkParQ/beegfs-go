package logger

import (
	"log/syslog"
	"strings"
)

// SyslogWriteSyncer implements [zapcore.WriteSyncer] allowing Zap output to be
// redirected to syslog.
type SyslogWriteSyncer struct {
	writer *syslog.Writer
}

// Return a new SyslogWriteSyncer that uses the log/syslog package to send
// messages to syslog. The priority is only used if the actual priority cannot
// be parsed from the log message. The prefix tag indicates the source of the
// log entry, typically name of the application. This does not need to be
// specified and will default to os.Args[0]
func NewSyslogWriteSyncer(priority syslog.Priority, tag string) (*SyslogWriteSyncer, error) {
	writer, err := syslog.New(priority, tag)
	if err != nil {
		return nil, err
	}
	return &SyslogWriteSyncer{
		writer: writer,
	}, nil
}

// Write() handles parsing out zap log messages and writing them into the syslog
// message format. This includes determining and writing out the appropriate
// severity level. NOTE: Currently the way we handle this translation is not
// expected to be the fastest approach and should eventually be updated if we
// ever need to log from areas where logging performance matters. Until then
// there is no point in prematurely optimizing our logging. If we did need to
// optimize, probably adding some additional buffering and wiring up Sync()
// would be one place to start.
func (s *SyslogWriteSyncer) Write(p []byte) (n int, err error) {

	// If the zapcore.Encoder was initialized with zapcore.NewConsoleEncoder()
	// then we expect messages in the following format:
	// <TIMESTAMP>\t<LEVEL>\t<STRING>.
	splitString := strings.Split(string(p), "\t")
	// Probably the message format changed, lets just write it to syslog as is:
	if len(splitString) < 3 {
		return s.writer.Write(p)
	}

	// Don't include the timestamp since syslog has its own timestamps.
	level := splitString[1]
	msg := strings.Join(splitString[2:], "")

	// Map the log levels defined by zapcore's level.go to the syslog severity
	// levels defined in RFC5424 (https://datatracker.ietf.org/doc/html/rfc5424).
	switch level {
	case "debug":
		return len(p), s.writer.Debug(msg)
	case "info":
		return len(p), s.writer.Info(msg)
	case "warn":
		return len(p), s.writer.Warning(msg)
	case "error":
		return len(p), s.writer.Err(msg)
	case "dpanic":
		return len(p), s.writer.Crit(msg)
	case "panic":
		return len(p), s.writer.Crit(msg)
	case "fatal":
		return len(p), s.writer.Crit(msg)
	default:
		// We don't know what level this was logged with.
		// Better we just log the original message.
		return s.writer.Write(p)
	}
}

// If we were working with a file based or buffered logger we may need to flush
// the buffer to disk. However the syslog package doesn't provide this
// mechanism, presumably because it is handling messages immediately. So we'll
// implement Sync() as a no-op.
func (s *SyslogWriteSyncer) Sync() error {
	return nil
}
