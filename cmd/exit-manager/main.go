package main

import (
	"bufio"
	"flag"
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
	fmt.Println("  t - Show current timeouts")
	fmt.Println("  ts <duration> - Set soft timeout (e.g., ts 10s)")
	fmt.Println("  th <duration> - Set hard timeout (e.g., th 30s)")
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
	return -1
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

func printTimeouts(em *exitmanager.ExitManager) {
	t := getTimeouts(em)
	fmt.Printf("[Timeouts] Soft: %v, Hard: %v\n", t.Soft, t.Hard)
}

func getTimeouts(em *exitmanager.ExitManager) exitmanager.TimeoutConfig {
	type timeouter interface {
		GetTimeouts() exitmanager.TimeoutConfig
	}
	if t, ok := any(em).(timeouter); ok {
		return t.GetTimeouts()
	}
	// fallback: not accessible
	return exitmanager.TimeoutConfig{}
}

func main() {
	softTimeout := flag.Duration("soft-timeout", time.Second*10, "Soft timeout duration (e.g., 10s)")
	hardTimeout := flag.Duration("hard-timeout", time.Second*30, "Hard timeout duration (e.g., 30s)")
	flag.Parse()

	em := exitmanager.Global()

	// Set initial timeouts from flags
	em.SetTimeouts(exitmanager.TimeoutConfig{Soft: *softTimeout, Hard: *hardTimeout})
	em.RegisterCleanup(func() { fmt.Println("[Cleanup] First cleanup executed!") })
	em.RegisterCleanup(func() { fmt.Println("[Cleanup] Second cleanup executed!") })

	fmt.Printf("Process ID: %d\n", os.Getpid())

	// Listen for real signals in background
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[Signal] Received real SIGINT/SIGTERM. Triggering shutdown...")
	}()

	printHelp()
	printState(em)
	printTimeouts(em)
	reader := bufio.NewReader(os.Stdin)

	locked := 0
	for {
		fmt.Print("\n> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		fields := strings.Fields(input)
		cmd := fields[0]
		switch cmd {
		case "l":
			if err := em.AcquireShutdownLock(); err == nil {
				locked++
				fmt.Println("[Action] Locked (in-flight work started)")
			} else {
				fmt.Println("[Warn] Cannot lock: already exiting!")
			}
			printState(em)
		case "u":
			if locked > 0 {
				em.ReleaseShutdownLock()
				locked--
				fmt.Println("[Action] Unlocked (work completed)")
			} else {
				fmt.Println("[Warn] No locks to unlock.")
			}
			printState(em)
		case "s":
			fmt.Println("[Action] Simulating SIGINT/SIGTERM...")
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(syscall.SIGINT)
		case "t":
			printTimeouts(em)
		case "ts":
			if len(fields) < 2 {
				fmt.Println("Usage: ts <duration>")
				continue
			}
			dur, err := time.ParseDuration(fields[1])
			if err != nil {
				fmt.Println("Invalid duration:", err)
				continue
			}
			t := getTimeouts(em)
			t.Soft = dur
			em.SetTimeouts(t)
			fmt.Println("[Timeout] Soft timeout set to", dur)
		case "th":
			if len(fields) < 2 {
				fmt.Println("Usage: th <duration>")
				continue
			}
			dur, err := time.ParseDuration(fields[1])
			if err != nil {
				fmt.Println("Invalid duration:", err)
				continue
			}
			t := getTimeouts(em)
			t.Hard = dur
			em.SetTimeouts(t)
			fmt.Println("[Timeout] Hard timeout set to", dur)
		case "q":
			fmt.Println("[Exit] Quitting immediately.")
			os.Exit(0)
		case "h":
			printHelp()
		default:
			fmt.Println("[Warn] Unknown command. Type 'h' for help.")
		}
	}
}
