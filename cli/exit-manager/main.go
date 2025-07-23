package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	exitmanager "github.com/mcwalrus/exit-manager"
)

func printHelp() {
	fmt.Println("\nExit Manager CLI Demo")
	fmt.Println("Commands:")
	fmt.Println("  l - Lock (simulate in-flight work)")
	fmt.Println("  u - Unlock (complete work)")
	fmt.Println("  s - Simulate SIGINT/SIGTERM (trigger shutdown)")
	fmt.Println("  q - Quit immediately")
	fmt.Println("  h - Help")
}

func printState(em *exitmanager.ExitManager) {
	fmt.Printf("\n[State] Locks: %d, Notified: %v\n", getLocks(em), getNotified(em))
}

// Helper to access private fields for demo (reflection or via exported methods if available)
func getLocks(em *exitmanager.ExitManager) int {
	type locker interface{ Locks() int }
	if l, ok := any(em).(locker); ok {
		return l.Locks()
	}
	// fallback: not available, so just return 0
	return 0
}

func getNotified(em *exitmanager.ExitManager) bool {
	n := false
	ch := em.Notify()
	select {
	case <-ch:
		n = true
	default:
		n = false
	}
	return n
}

func main() {
	timeout := 10 * time.Second
	em := exitmanager.Register(timeout)

	em.Cleanup(func() { fmt.Println("[Cleanup] First cleanup executed!") })
	em.Cleanup(func() { fmt.Println("[Cleanup] Second cleanup executed!") })

	// Listen for real signals in background
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[Signal] Received real SIGINT/SIGTERM. Triggering shutdown...")
		// This will trigger the exit manager's shutdown
	}()

	printHelp()
	printState(em)
	reader := bufio.NewReader(os.Stdin)

	locked := 0
	for {
		fmt.Print("\n> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		switch input {
		case "l":
			if em.LockInc() {
				locked++
				fmt.Println("[Action] Locked (in-flight work started)")
			} else {
				fmt.Println("[Warn] Cannot lock: already exiting!")
			}
			printState(em)
		case "u":
			if locked > 0 {
				em.LockDec()
				locked--
				fmt.Println("[Action] Unlocked (work completed)")
			} else {
				fmt.Println("[Warn] No locks to unlock.")
			}
			printState(em)
		case "s":
			fmt.Println("[Action] Simulating SIGINT/SIGTERM...")
			// Send a signal to our own process
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(syscall.SIGINT)
			// The exitmanager will handle shutdown
		case "q":
			fmt.Println("[Exit] Quitting immediately.")
			os.Exit(0)
		case "h":
			printHelp()
		case "":
			// ignore
		default:
			fmt.Println("[Warn] Unknown command. Type 'h' for help.")
		}
	}
}
