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
	fmt.Println("  s - Shutdown Action (trigger shutdown)")
	fmt.Println("  p - Print state of exit manager")
	fmt.Println("  st <duration> - Set timeout (e.g., st 10s, use 0s for no timeout)")
	fmt.Println("  q - Quit immediately")
	fmt.Println("  h - Help")
	fmt.Println("\nTip: Try 'l' to acquire locks, then 's' to see graceful shutdown in action!")
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
	timeout := flag.Duration("timeout", time.Second*10, "Timeout duration (e.g., 10s)")
	flag.Parse()

	em := exitmanager.Global()

	// Set initial timeouts from flags
	em.SetTimeout(*timeout)
	em.RegisterCleanup(func() { fmt.Println("[Cleanup] First cleanup executed!") })
	em.RegisterCleanup(func() { fmt.Println("[Cleanup] Second cleanup executed!") })

	printState := func(em *exitmanager.ExitManager) {
		currentTimeout := "no timeout"
		if *timeout > 0 {
			currentTimeout = timeout.String()
		}
		fmt.Printf("\n[State] Active Locks: %d | Timeout: %s | Shutdown Notified: %v\n",
			em.Locks(), currentTimeout, getNotified(em))
	}

	fmt.Printf("Exit Manager CLI Demo - Process ID: %d\n", os.Getpid())
	fmt.Println("This demo shows graceful shutdown coordination in action.")

	// Listen for real signals in background
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[Signal] Received real SIGINT/SIGTERM. Triggering shutdown...")
		em.Shutdown()
	}()

	printHelp()
	printState(em)
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
				fmt.Printf("[Action] 🔒 Lock acquired! (Total locks: %d)\n", locked)
			} else {
				fmt.Println("[Warn] ❌ Cannot acquire lock: shutdown already in progress!")
			}
			printState(em)
		case "u":
			if locked > 0 {
				em.ReleaseShutdownLock()
				locked--
				fmt.Printf("[Action] 🔓 Lock released! (Remaining locks: %d)\n", locked)
			} else {
				fmt.Println("[Warn] ⚠️  No locks to release.")
			}
			printState(em)
		case "s":
			fmt.Println("[Action] 🚀 Triggering shutdown...")
			if locked > 0 {
				fmt.Printf("[Info] Will wait for %d active locks to be released...\n", locked)
			}
			em.Shutdown()
		case "p":
			printState(em)
		case "st":
			if len(fields) < 2 {
				fmt.Println("[Usage] st <duration> (e.g., st 30s, st 0s for no timeout)")
				continue
			}
			dur, err := time.ParseDuration(fields[1])
			if err != nil {
				fmt.Println("[Error] Invalid duration format:", err)
				fmt.Println("[Help] Use formats like: 10s, 2m, 1h30m, or 0s for no timeout")
				continue
			}
			timeout = &dur
			em.SetTimeout(*timeout)
			if dur == 0 {
				fmt.Println("[Timeout] ⏰ Timeout disabled (will wait indefinitely)")
			} else {
				fmt.Printf("[Timeout] ⏰ Timeout set to %v\n", dur)
			}
			printState(em)
		case "q":
			fmt.Println("[Exit] 👋 Quitting immediately (bypassing graceful shutdown).")
			os.Exit(0)
		case "h":
			printHelp()
		default:
			fmt.Printf("[Warn] ❓ Unknown command '%s'. Type 'h' for help.\n", cmd)
		}
	}
}
