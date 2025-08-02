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
	fmt.Println("  st <duration> [mode] - Set timeout (e.g., st 10s none, st 5s graceful, st 3s forceful)")
	fmt.Println("                        Modes: none (default), graceful, forceful")
	fmt.Println("                        Use 0s to disable timeout")
	fmt.Println("  q - Quit immediately")
	fmt.Println("  h - Help")
	fmt.Println("\nTimeout Modes:")
	fmt.Println("  none     - No timeout (default) - waits indefinitely")
	fmt.Println("  graceful - Timeout applies only to cleanup functions")
	fmt.Println("  forceful - Timeout applies to entire shutdown process")
	fmt.Println("\nTip: Try 'l' to acquire locks, then 's' to see graceful shutdown!")
	fmt.Println("Tip: Try different timeout modes to see behavior differences!")
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

func timeoutModeToString(mode exitmanager.TimeoutMode) string {
	switch mode {
	case exitmanager.TimeoutModeNone:
		return "none"
	case exitmanager.TimeoutModeGraceful:
		return "graceful"
	case exitmanager.TimeoutModeForceful:
		return "forceful"
	default:
		return "unknown"
	}
}

func parseTimeoutMode(modeStr string) exitmanager.TimeoutMode {
	switch strings.ToLower(modeStr) {
	case "none":
		return exitmanager.TimeoutModeNone
	case "graceful":
		return exitmanager.TimeoutModeGraceful
	case "forceful":
		return exitmanager.TimeoutModeForceful
	default:
		return exitmanager.TimeoutModeNone
	}
}

func main() {
	timeout := flag.Duration("timeout", time.Second*10, "Timeout duration (e.g., 10s)")
	modeFlag := flag.String("mode", "none", "Timeout mode: none, graceful, forceful")
	flag.Parse()

	em := exitmanager.Global()
	currentMode := parseTimeoutMode(*modeFlag)
	currentTimeout := *timeout

	// Set initial timeout from flags
	em.SetTimeout(currentMode, *timeout)
	em.RegisterCleanup(func() {
		fmt.Println("[Cleanup] First cleanup executed!")
		time.Sleep(100 * time.Millisecond)
	})
	em.RegisterCleanup(func() {
		fmt.Println("[Cleanup] Second cleanup executed!")
		time.Sleep(100 * time.Millisecond)
	})

	printState := func(em *exitmanager.ExitManager) {
		timeoutStr := "disabled"
		if currentTimeout > 0 {
			timeoutStr = currentTimeout.String()
		}
		fmt.Printf("\n[State] Active Locks: %d | Timeout: %s (%s mode) | Shutdown Notified: %v\n",
			em.Locks(), timeoutStr, timeoutModeToString(currentMode), getNotified(em))
	}

	fmt.Printf("Exit Manager CLI Demo - Process ID: %d\n", os.Getpid())
	fmt.Println("This demo shows graceful shutdown coordination with timeout modes.")

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
			if currentTimeout > 0 {
				fmt.Printf("[Info] Timeout mode: %s (%s)\n", timeoutModeToString(currentMode), currentTimeout)
				switch currentMode {
				case exitmanager.TimeoutModeNone:
					fmt.Println("[Info] Will wait indefinitely (timeout disabled)")
				case exitmanager.TimeoutModeGraceful:
					fmt.Println("[Info] Timeout applies only to cleanup functions")
				case exitmanager.TimeoutModeForceful:
					fmt.Println("[Info] Timeout applies to entire shutdown process")
				}
			}
			em.Shutdown()
		case "p":
			printState(em)
		case "st":
			if len(fields) < 2 {
				fmt.Println("[Usage] st <duration> [mode]")
				fmt.Println("        Examples: st 30s, st 10s graceful, st 5s forceful")
				fmt.Println("        Modes: none, graceful, forceful")
				fmt.Println("        Use 0s to disable timeout")
				continue
			}
			dur, err := time.ParseDuration(fields[1])
			if err != nil {
				fmt.Println("[Error] Invalid duration format:", err)
				fmt.Println("[Help] Use formats like: 10s, 2m, 1h30m, or 0s for no timeout")
				continue
			}

			mode := currentMode
			if len(fields) >= 3 {
				mode = parseTimeoutMode(fields[2])
				if fields[2] != "" && mode == exitmanager.TimeoutModeNone && strings.ToLower(fields[2]) != "none" {
					fmt.Printf("[Warn] Unknown timeout mode '%s', using 'none'. Available: none, graceful, forceful\n", fields[2])
				}
			}

			currentTimeout = dur
			currentMode = mode
			em.SetTimeout(currentMode, currentTimeout)

			if dur <= 0 {
				fmt.Println("[Timeout] ⏰ Timeout disabled (will wait indefinitely)")
			} else {
				fmt.Printf("[Timeout] ⏰ Timeout set to %v in %s mode\n", dur, timeoutModeToString(currentMode))
				switch currentMode {
				case exitmanager.TimeoutModeNone:
					fmt.Println("           Mode explanation: No timeout - waits indefinitely")
				case exitmanager.TimeoutModeGraceful:
					fmt.Println("           Mode explanation: Timeout applies only to cleanup functions")
				case exitmanager.TimeoutModeForceful:
					fmt.Println("           Mode explanation: Timeout applies to entire shutdown process")
				}
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
