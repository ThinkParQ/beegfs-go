package util

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/term"
)

// TermRefresher allows output printed to the terminal to be periodically refreshed for commands
// that want to provide functionality similar to the Linux watch command. To use it first call
// StartRefresh() to start collecting everything that would normally be printed to stdout so
// functions like fmt.Print can continue to be used normally. When ready to clear the screen and
// print the buffered output along with an optional footer, call `FinishRefresh()`.
//
// Note this works by temporarily redirecting stdout to avoid flickering effects that are more
// prevalent on some terminals depending on the time between print statements. Alternatively this
// could have accepted the output as a string, but this makes it more difficult to integrate the
// refresh functionality with functions that already use print to generate output, or may want to
// sometimes immediately print output and other times periodically gather and print output.
//
// IMPORTANT: You MUST call FinishRefresh() after calling StartRefresh() to cleanup and ensure the
// original stdout is restored.
type TermRefresher struct {
	originalStdout *os.File
	pipeIn         *os.File
	pipeOut        *os.File
	width          int
	height         int
}

type termWatcherOpts struct {
	Footer string
	Cancel bool
}

type termWatcherOpt func(*termWatcherOpts)

func WithTermFooter(footer string) termWatcherOpt {
	return func(args *termWatcherOpts) {
		args.Footer = footer
	}
}

func WithCancelRefresh() termWatcherOpt {
	return func(args *termWatcherOpts) {
		args.Cancel = true
	}
}

// Start a refresh. If this does not return an error FinishRefresh() MUST be called even if the
// caller decided not to print the output for some reason.
func (t *TermRefresher) StartRefresh() error {
	var err error
	t.width, t.height, err = term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("error determining terminal size (is stdout a terminal?): %w", err)
	}
	t.pipeOut, t.pipeIn, err = os.Pipe()
	if err != nil {
		return fmt.Errorf("error setting up internal pipe: %w", err)
	}
	t.originalStdout = os.Stdout
	os.Stdout = t.pipeIn
	return nil
}

// Print all output that was sent to stdout since the last StartRefresh() call. Optionally provide a
// footer using `WithTermFooter()` or discard all buffered output and don't print anything by
// providing `WithCancelRefresh()`.
func (t *TermRefresher) FinishRefresh(opts ...termWatcherOpt) error {
	args := &termWatcherOpts{}
	for _, opt := range opts {
		opt(args)
	}
	var buf bytes.Buffer
	t.pipeIn.Close()
	io.Copy(&buf, t.pipeOut)
	t.pipeOut.Close()
	os.Stdout = t.originalStdout
	if !args.Cancel {
		// Clears the terminal.
		fmt.Print("\033[H\033[2J")
		if args.Footer != "" {
			// Otherwise the top line may be cut off by the footer.
			fmt.Println()
		}
		fmt.Print(buf.String())
		t.printFooter(args.Footer, t.width, t.height)
	}
	return nil
}

func (t *TermRefresher) printFooter(footerText string, width int, height int) {
	spaces := func(n int) string {
		if n < 0 {
			n = 0
		}
		return fmt.Sprintf("%*s", n, "")
	}
	// Move the cursor to the bottom of the screen
	fmt.Printf("\033[%d;1H", height)
	// Set the background color close to #FAB800.
	fmt.Print("\033[48;5;220m\033[30m")
	// Print the footer, padded to the width of the terminal
	fmt.Printf("%s%s", footerText, spaces(width-len(footerText)))
	// Reset background color
	fmt.Print("\033[0m")
}

// FlashTerminal() triggers a terminal bell notification and flashes the terminal.
func FlashTerminal() {
	// Trigger a terminal bell notification (\a):
	fmt.Print("\n\a")
	// Flash the terminal:
	fmt.Print("\033[?5h")
	time.Sleep(100 * time.Millisecond)
	fmt.Print("\033[?5l")
}
