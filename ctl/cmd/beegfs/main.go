package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/thinkparq/beegfs-go/ctl/internal/cmd"
)

func main() {
	if os.Geteuid() == 0 {
		// If the CTL binary was installed using a package it will have the setgid bit set. This
		// causes Go programs to run in "secure mode" and act as though GOTRACEBACK=none is set
		// (https://github.com/golang/go/issues/62474#issuecomment-1708447731) which means stack
		// traces are suppressed for security reasons (https://github.com/golang/go/issues/60272).
		// Because this makes debugging panics challenging, if the user has root privileges anyway,
		// reset GOTRACEBACK back to the default "single" configuration.
		//
		// Note the GOTRACEBACK environment variable could still be used, for example to print stack
		// traces for "all" goroutines.
		debug.SetTraceback("single")
	} else if os.Getegid() != os.Getgid() || os.Geteuid() != os.Getuid() {
		// Otherwise if the effective user or group ID does not match the current user/group/ then
		// if a panic happens we know there probably won't be any traceback since the setgid bit is
		// likely set. Print some help text as this may be confusing otherwise to troubleshoot.
		defer func() {
			// Note recover only catches panics from the same goroutine in which its called. This
			// means panics outside the main goroutine will return the panic without the extra text.
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "panic: %s (stack traces may be suppressed since the effective user is not root)\n", r)
			}
		}()
	}
	os.Exit(cmd.Execute())
}
