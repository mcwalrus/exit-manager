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
	fmt.Println("  p - Print state of exit maanger")
	fmt.Println("  st <duration> - Set timeout (e.g., ts 10s, less than 0 for no timeout)")
	fmt.Println("  q - Quit immediately")
	fmt.Println("  h - Help")
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
		fmt.Printf("\n[State] Locks: %d, Timeout: %v, Notified: %v\n", em.Locks(), *timeout, getNotified(em))
	}

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
			em.ReleaseShutdownLock()
			if locked > 0 {
				locked--
				fmt.Println("[Action] Unlocked (work completed)")
			} else {
				fmt.Println("[Warn] No locks to unlock.")
			}
			printState(em)
		case "s":
			fmt.Println("[Action] Shutdown...")
			em.Shutdown()
		case "st":
			if len(fields) < 2 {
				fmt.Println("Usage: st <duration>")
				continue
			}
			dur, err := time.ParseDuration(fields[1])
			if err != nil {
				fmt.Println("Invalid duration:", err)
				continue
			}
			timeout = &dur
			em.SetTimeout(*timeout)
			fmt.Println("[Timeout] timeout set to", dur)
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
