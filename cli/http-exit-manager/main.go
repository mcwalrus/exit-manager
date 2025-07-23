package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
	// Create ExitManager with 10s timeout
	em := exitmanager.New(10 * time.Second)

	// Health endpoint with ExitManager middleware
	healthHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.Handle("/health", em.HttpHealthCheckMiddleware()(healthHandler))

	// Request endpoint with ExitManager middleware
	requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("/request: started")
		time.Sleep(2 * time.Second)
		fmt.Fprintln(w, "request complete")
		log.Println("/request: finished")
	})
	http.Handle("/request", em.HttpRequestMiddleware()(requestHandler))

	server := &http.Server{Addr: ":8080"}

	// Register cleanup to shutdown HTTP server
	em.Cleanup(func() {
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
	fmt.Println("Commands: l=lock, u=unlock, h=GET /health, r=GET /request, q=quit (SIGINT)")
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		switch input {
		case "l":
			if em.TryLock() {
				fmt.Println("Locked exit manager (simulating in-flight work)")
			} else {
				fmt.Println("Could not lock: already shutting down")
			}
		case "u":
			em.Unlock()
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
	<-em.Notify()
	log.Println("Exit manager shutdown complete")
}
