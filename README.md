# Exit Manager

A Go library that provides **graceful shutdown coordination** for applications, ensuring critical operations complete before process termination. The libraries presents a simple yet powerful interface, providing cleanup handling, ensuring process exit, and managed HTTP server shutdowns. This library achieves all its functionality with zero external dependencies. For more information, please see the documentation at: https://pkg.go.dev/github.com/mcwalrus/exit-manager.

## Features

1. **🔄 Signal Handling**: Registers listener for SIGINT (Ctrl+C) and SIGTERM
2. **📡 Notifications**: When shutdown starts, the `Notify()` channel closes
3. **🔒 Lock Coordination**: Waits for all shutdown locks to be released
4. **🧹 Cleanup Execution**: Runs cleanup functions in reverse registration order  
5. **🚪 Process Exit**: Terminates with exit code 0 (success) or 1 (timeout)
6. **🌐 HTTP Server Support**: Graceful shutdown coordination for HTTP servers

## Installation

Compatible with Go 1.14+ versions:

```bash
go get github.com/mcwarlus/exit-manager
```


## Try CLI First!

Before diving into the API examples, try the interactive CLI to see graceful shutdown in action:

```bash
cd cmd/exit-manager
go run main.go
```

**Quick Demo Scenario:**
1. Run the script: `go run`.
2. Type `l` (enter) a few times to acquire locks
3. Type `s` (enter) to trigger shutdown - notice it waits for locks!
4. Type `u` (enter) to release locks and watch cleanup execute
5. Or try Ctrl+C to see real signal handling

The CLI will provide a hands-on experience that will help you understand the concepts before reading the code examples below.

## Testing

Test with race condition detection:

```bash
go test -race .
```

## API

### Timeout Protection

Prevent hanging during shutdown by setting a timeout:

```go
em := exitmanager.Global()
em.SetTimeout(30 * time.Second) // Force exit after 30 seconds

// If cleanup takes too long, the process exits with code 1
// If cleanup completes normally, the process exits with code 0
```

### Programmatic Shutdown

Trigger shutdown from your code:

```go
em := exitmanager.Global()

// Trigger shutdown programmatically (same as Ctrl+C)
go func() {
    time.Sleep(10 * time.Second)
    em.Shutdown()
}()

<-em.Notify() // Will trigger after 10 seconds
```

### Monitoring Active Locks

Check how many operations are preventing shutdown:

```go
em := exitmanager.Global()

// Check active locks
if locks := em.Locks(); locks > 0 {
    log.Printf("Waiting for %d operations to complete...", locks)
}
```

## Basic Usage

### Shutdown Routinues

Use when you have go routinues which you want to exit on notified shutdown.

```go
package main

import (
    "log"
    "time"

    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()

    // Start a background worker
    go backgroundWorker(em)

    // Wait for shutdown signal (Ctrl+C)
    <-em.Notify()
    log.Println("Shutdown signal received, exiting gracefully...")

    // Avoids exit on main routine
    select {}
}

func backgroundWorker(em *exitmanager.ExitManager) {
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-em.Notify():
            log.Println("Worker: shutdown signal received, stopping...")
            return
        case <-ticker.C:
            log.Println("Worker: doing work...")
        }
    }
}
```

### Shutdown Locks

Use when you have operations that must complete before shutdown. The program won't exit until all shutdown locks are released.

```go
package main

import (
    "log"
    "time"

    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()

    // Start workers that do critical operations
    for i := 0; i < 3; i++ {
        go criticalWorker(em, i)
    }

    // Wait for shutdown signal
    <-em.Notify()
    log.Println("Shutdown initiated, waiting for critical operations...")
    
    // Avoids exit on main routine
    select {}
}

func criticalWorker(em *exitmanager.ExitManager, id int) {
    for {
        // Check if shutdown has started
        select {
        case <-em.Notify():
            log.Printf("Worker %d: received shutdown signal, exiting", id)
            return
        default:
        }

        // Protect critical operation with a shutdown lock
        if err := em.AcquireShutdownLock(); err != nil {
            log.Printf("Worker %d: shutdown in progress, cannot start new work", id)
            return
        }

        // Simulate work ...
        log.Printf("Worker %d: starting critical operation...", id)
        time.Sleep(3 * time.Second)
        log.Printf("Worker %d: critical operation complete", id)
        em.ReleaseShutdownLock()

        // Small delay before next operation
        time.Sleep(1 * time.Second)
    }
}
```

### Cleanup Functions

Use when you need to clean up resources (close files, database connections, etc.) in a specific order. Registered cleanup functions with the exit manager run in reverse order.

```go
package main

import (
    "log"
    "time"

    exitmanager "github.com/mcwalrus/exit-manager"
)

// Simulate resources that need cleanup
var database *Database
var cache *Cache

type Database struct{ name string }
func (db *Database) Close() { log.Printf("Closing %s database...", db.name) }

type Cache struct{ name string }
func (c *Cache) Close() { log.Printf("Closing %s cache...", c.name) }

func main() {
    em := exitmanager.Global()

    // Initialise resources
    database = &Database{name: "user"}
    cache = &Cache{name: "session"}

    // Register cleanup functions
    em.RegisterCleanup(func() {
        log.Println("LIFO: first-in, last-out...")
    })

    em.RegisterCleanup(func() {
        log.Println("Closing database connections...")
        database.Close()
    })

    em.RegisterCleanup(func() {
        log.Println("Close cache connections...") 
        cache.Close()
    })

    log.Println("Application running... Press Ctrl+C to shutdown")
    
    // Wait for shutdown
    <-em.Notify()
    log.Println("Shutdown started...")
    
    // Exit manager will run cleanup functions before exiting
    select {}
}
```

### Context Integration

Use when you have long-running operations that should be cancelled on shutdown. When shutdown starts, registered contexts with the exit manager are cancelled, cleanly stopping the long-running operation.

```go
package main

import (
    "context"
    "log"
    "time"

    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()

    // Register a context that is cancelled on shutdown
    ctx, cancel := em.WithCancel(context.Background())
    defer cancel()

    // Waits for long-running task
    em.RegisterCleanup(func() {
        time.Sleep(100 * time.Millisecond)
    })

    // Start long-running operation
    go longRunningTask(ctx)

    // Wait for shutdown
    <-em.Notify()
    log.Println("Shutdown initiated...")
    
    // The context is automatically cancelled, stopping the long-running task
    select {}
}

func longRunningTask(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            log.Println("Long-running task: context cancelled, stopping...")
            return
        case <-ticker.C:
            log.Println("Long-running task: processing...")
        }
    }
}
```

### Global Access

An example showing different routines accessing the global exit manager:

```go
package main

import (
    "context"
    "log"
    "time"

    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    em.SetTimeout(10 * time.Second)
    
    // Waits for background worker on exit
    em.RegisterCleanup(func() {
        time.Sleep(100 * time.Millisecond)
    })
    go backgroundWorker()

    log.Println("Services started. Press Ctrl+C to shutdown...")
    <-em.Notify()
    
    log.Println("Shutdown initiated")
    select {}
}

// backgroundWorker uses global exit manager
func backgroundWorker() {
    em := exitmanager.Global()
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-em.Notify():
            log.Println("Worker: shutdown signal received, stopping")
            return
        case <-ticker.C:
            log.Println("Background work...")
        }
    }
}
```

## HTTP Server Integration

For applications with HTTP servers, use the `httpexit.HTTPExitManager` for coordinated shutdowns. Through the `ExitManager`, HTTP servers are to be shutdown before any other shutdown operation. Pre-shutdown hooks will execute before HTTP servers begin shutdowns occur, which is useful for closing hijacked server connections (such as websockets) ahead of server shutdowns if required.

### HTTP Server Shutdowns

Multiple HTTP servers shutdown occur concurrentlyfor faster overall shutdown:

```go
// Register exit manager
em := exitmanager.Global()
httpEM := em.RegisterHTTPExitManager()

// Main API server
apiServer := &http.Server{Addr: ":8080", Handler: apiHandler}
httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
    Server:  apiServer,
    Timeout: 30 * time.Second,
})

// Metrics server
metricsServer := &http.Server{Addr: ":8081", Handler: metricsServer}
httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
    Server:  metricsServer,
    Timeout: 10 * time.Second,
})
// Both servers will shutdown concurrently when exit is triggered
<-em.Notify()
```

### HTTP Pre-Shutdown Hooks

Pre-shutdown hooks execute before HTTP servers begin shutting down which is useful for closing hijacked server connections ahead of server shutdowns:

```go
// Register exit manager
em := exitmanager.Global()
httpEM := em.RegisterHTTPExitManager()

// Multiple pre-shutdown hooks can be registered
httpEM.RegisterCleanup(func() {
    log.Println("LIFO: first-in, last-out...")
})

httpEM.RegisterPreShutdown(func() {
    log.Println("Closing websocket connections...")
    // Close websocket connections
})
```

### HTTP Shutdown Example

```go
package main

import (
    "log"
    "net/http"
    "time"
    
    exitmanager "github.com/mcwalrus/exit-manager"
    httpexit "github.com/mcwalrus/exit-manager/http-exit"
)

func main() {
    // Create HTTP server
    server := &http.Server{
        Addr:    ":8080",
        Handler: myHandler(),
    }

    // Set up HTTP exit manager
    em := exitmanager.Global()
    httpEM := em.RegisterHTTPExitManager()
    
    // Register pre-shutdown hook for WebSocket connections
    httpEM.RegisterPreShutdown(func() {
        log.Println("Closing WebSocket connections...")
        closeActiveWebSockets()
    })
    
    // Register HTTP server for graceful shutdown
    err := httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
        Server:  server,
        Timeout: 30 * time.Second,
        HandleErr: func(err error) {
            log.Printf("Server shutdown error: %v", err)
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Start server
    go func() {
        log.Println("Server starting on :8080")
        if err := server.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatal(err)
        }
    }()

    // Wait for shutdown signal
    <-em.Notify()
    log.Println("Shutdown initiated, servers shutting down gracefully...")
    
    // Exit manager handles the rest
    select {}
}

func myHandler() http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        w.Write([]byte("Hello World"))
    })
    return mux
}

func closeActiveWebSockets() {
    // Implement close of active WebSocket connections
    // Ensures hijacked WebSocket connections don't prevent graceful shutdown
}
```

## Contributing

Report issues and feature requests at the [GitHub repository](https://github.com/mcwalrus/exitmanager). 

I'm open to any ideas of shutdown integrations this module can provide for the standard libary.

## License

This module is available under the MIT License.