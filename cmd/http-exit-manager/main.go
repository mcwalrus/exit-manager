package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	exitmanager "github.com/mcwalrus/exit-manager"
)

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
	return exitmanager.TimeoutConfig{}
}

func main() {
	softTimeout := flag.Duration("soft-timeout", 0, "Soft timeout duration (e.g., 10s)")
	hardTimeout := flag.Duration("hard-timeout", 0, "Hard timeout duration (e.g., 30s)")
	flag.Parse()

	em := exitmanager.Global()

	em.SetTimeouts(exitmanager.TimeoutConfig{Soft: *softTimeout, Hard: *hardTimeout})

	// Health endpoint with ExitManager middleware
	healthHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.Handle("/health", em.HTTPServiceUnavailableMiddleware()(healthHandler))

	// Request endpoint with ExitManager middleware
	requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("/request: started")
		time.Sleep(10 * time.Second)
		fmt.Fprintln(w, "request complete")
		log.Println("/request: finished")
	})
	http.Handle("/request", em.HTTPGracefulShutdownMiddleware()(requestHandler))

	server := &http.Server{Addr: ":8080"}
	em.RegisterHTTPServer(server)

	// Register cleanup to shutdown HTTP server
	em.RegisterCleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		log.Println("HTTP server shutdown complete")
	})

	// Start HTTP server
	go func() {
		log.Println("HTTP exit manager demo running on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe: %v", err)
		}
	}()

	// CLI loop for key controls
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Commands: l=lock, u=unlock, h=GET /health, r=GET /request, t=show timeouts, ts=set soft timeout, th=set hard timeout, q=quit (SIGINT)")
	for {
		fmt.Print("> ")
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
				fmt.Println("Locked exit manager (simulating in-flight work)")
			} else {
				fmt.Println("Could not lock: already shutting down")
			}
		case "u":
			em.ReleaseShutdownLock()
			fmt.Println("Unlocked exit manager")
		case "h":
			resp, err := http.Get("http://localhost:8080/health")
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			defer resp.Body.Close()
			fmt.Printf("/health: %d\n", resp.StatusCode)
		case "r":
			resp, err := http.Get("http://localhost:8080/request")
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			defer resp.Body.Close()
			fmt.Printf("/request: %d\n", resp.StatusCode)
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
			fmt.Println("Sending SIGINT (Ctrl+C)...")
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		default:
			fmt.Println("Unknown command")
		}
	}

	// Wait for exit manager to finish (blocks until shutdown)
	// <-em.Notify()
	// log.Println("Exit manager shutdown complete")
}
