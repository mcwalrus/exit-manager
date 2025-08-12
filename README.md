# Exit Manager

A Go library that provides **graceful shutdown coordination** for applications, ensuring critical operations and cleanup completes before process termination. For more information, please see the documentation at: https://pkg.go.dev/github.com/mcwalrus/exit-manager.

## Features

1. **🔄 Signal Handling**: Registers listener for SIGINT (Ctrl+C) and SIGTERM
2. **📡 Notifications**: When shustdown starts, the `Notify()` channel closes
3. **🌐 Shutdown Servers**: Concurrent shutdowns for HTTP servers
4. **🔒 Lock Coordination**: Waits for all shutdown locks to be released
5. **🧹 Cleanup Execution**: Runs cleanup functions in reverse registration order
6. **🚪 Process Exit**: Terminates with exit code 0 (success) or 1 (timeout)

## Installation

Compatible with Go 1.21+ versions:

```bash
go get github.com/mcwarlus/exit-manager
```


## Try CLI First!

Before diving into the API examples, try the interactive CLI to see graceful shutdown in action:

```bash
cd cmd/exit-manager && go run .
```

The CLI will provide a hands-on experience that will help you understand the timeout modes and concepts before reading the code examples below.

## Testing

Test with race condition detection:

```bash
go test -race ./...
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

    // Avoid exit on main routine
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
    
    // Avoid exit on main routine
    select {}
}

func criticalWorker(em *exitmanager.ExitManager, id int) {
    for {
        // Acquire lock to prevent shutdown
        if err := em.AcquireShutdownLock(); err != nil {
            log.Printf("Worker %d: shutdown in progress, cannot start new work", id)
            return
        }

        // Simulate work ...
        log.Printf("Worker %d: starting critical operation...", id)
        time.Sleep(3 * time.Second)
        log.Printf("Worker %d: critical operation complete", id)
        em.ReleaseShutdownLock()

        // Check if shutdown has started
        select {
        case <-em.Notify():
            log.Printf("Worker %d: received shutdown signal, exiting", id)
            return
        case <-time.After(1 * time.Second):
        }
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
    ctx, cancel := em.NotifyContext(context.Background())
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

### HTTP Server Graceful Shutdown

Use when you have HTTP servers that need to be shutdown gracefully. The exit manager can coordinate multiple servers and pre-shutdown handlers for complex scenarios like closing hijacked connections or notifying load balancers.

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    
    // Configure HTTP server shutdown timeout
    em.SetServerTimeout(30 * time.Second)
    
    // Create HTTP servers
    mainServer := &http.Server{
        Addr:    ":8080",
        Handler: http.DefaultServeMux,
    }
    
    adminServer := &http.Server{
        Addr:    ":8081", 
        Handler: http.DefaultServeMux,
    }
    
    // Register pre-shutdown handler for custom cleanup
    em.RegisterPreShutdown(func() {
        log.Println("Notifying load balancer of shutdown...")
        // Custom logic to notify load balancer
        time.Sleep(2 * time.Second)
    })
    
    // Register servers for graceful shutdown
    if err := em.RegisterServer(mainServer); err != nil {
        log.Fatal("Failed to register main server:", err)
    }
    
    if err := em.RegisterServer(adminServer); err != nil {
        log.Fatal("Failed to register admin server:", err)
    }
    
    // Setup routes
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Hello World!"))
    })
    
    http.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Admin Panel"))
    })
    
    // Start servers
    go func() {
        log.Println("Starting main server on :8080")
        if err := mainServer.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatal("Main server error:", err)
        }
    }()
    
    go func() {
        log.Println("Starting admin server on :8081")
        if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
            log.Fatal("Admin server error:", err)
        }
    }()
    
    log.Println("Servers started. Press Ctrl+C to shutdown gracefully...")
    
    // Wait for shutdown signal
    <-em.Notify()
    log.Println("Shutdown initiated, gracefully stopping servers...")
    
    // Exit manager handles server shutdown automatically
    select {}
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
    em.SetTimeout(exitmanager.TimeoutModeForceful, 10*time.Second)
    
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

## Contributing

Report issues and feature requests at the [GitHub repository](https://github.com/mcwalrus/exitmanager). 

I'm open to any ideas of shutdown integrations this module can provide for the standard libary.

## License

This module is available under the MIT License.