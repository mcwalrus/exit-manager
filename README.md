# Exit Manager

`exitmanager` is a Go library that provides graceful shutdown coordination for applications, ensuring critical operations complete before process termination.

## Key Features

- 🛡️ **Safe Shutdown**: Prevents shutdown during critical operations using locks
- 🔄 **Signal Handling**: Automatically listens for SIGINT/SIGTERM or shutdown signals
- 🎯 **Context Integration**: Register cancellation contexts on notified shutdown
- 🧹 **Cleanup Coordination**: Executes cleanup functions in reverse registration order
- 📡 **Shutdown Notifications**: Notify goroutines when notified shutdown begins
- **Server Registrations**: Handles graceful server shutdown before coordinated cleanup
- ⏱️ **Timeout Support**: Configurable forced exit after timeout

## Installation

```bash
go get github.com/mcwarlus/exit-manager
```

## When to Use?

**1) Critical Process Management**

Use exit-manager when you have multiple goroutines performing critical work that must complete before the application shuts down. The lock system ensures no operations are interrupted mid-process unless a timeout is set.

**2) Resource Cleanup**

Use exit-manager when you need to guarantee cleanup functions execute in a specific order during shutdown (database connections, file handles, network connections, etc).

**3) Graceful Server Shutdowns**

Use exit-manager for web servers, micro-services, or any long-running application that needs to handle shutdown signals gracefully while completing in-flight requests.

## Quick Start

### Basic Usage

```go
package main

import (
    "log"
    "time"
    "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()

    // Register cleanup functions
    em.RegisterCleanup(func() {
        log.Println("Closing database connections...")
    })

    em.RegisterCleanup(func() {
        log.Println("Stopping background workers...")
    })

    // Start worker goroutines
    for i := 0; i < 3; i++ {
        go doWork(i)
    }

    // Set a timeout for forced shutdown
    em.SetTimeout(30 * time.Second)

    // Wait for shutdown signal
    <-em.Notify()
    log.Println("Shutdown initiated, waiting for workers...")
    
    // Wait for manager to exit process
    select {}
}

func doWork(id int) {
    em := exitmanager.Global()
    
    for {
        // Protect critical operation
        if err := em.AcquireShutdownLock(); err != nil {
            log.Printf("Worker %d: shutdown in progress, exiting", id)
            return
        }
        
        // Simulate work
        log.Printf("Worker %d: processing...", id)
        time.Sleep(2 * time.Second)
        
        em.ReleaseShutdownLock()
        
        // Check if shutdown was initiated
        select {
        case <-em.Notify():
            log.Printf("Worker %d: received shutdown signal", id)
            return
        default:
            // Continue working
        }
    }
}
```

### Context Integration

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    
    // Create context that cancels on shutdown
    ctx, cancel := em.WithCancel(context.Background())
    defer cancel()
    
    go backgroundTask(ctx)
    
    // Simulate some work
    time.Sleep(10 * time.Second)
    em.Shutdown()
}

func backgroundTask(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            log.Println("Background task received shutdown signal")
            return
        case <-ticker.C:
            log.Println("Background task working...")
        }
    }
}
```

### HTTP Server Example

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"
    "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    
    server := &http.Server{
        Addr:    ":8080",
        Handler: http.HandlerFunc(handleRequest),
    }
    
    // Register server shutdown
    em.RegisterCleanup(func() {
        log.Println("Shutting down HTTP server...")
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        server.Shutdown(ctx)
    })
    
    // Set overall timeout
    em.SetTimeout(30 * time.Second)
    
    go func() {
        log.Println("Starting server on :8080")
        server.ListenAndServe()
    }()
    
    // Wait for shutdown
    <-em.Notify()
    log.Println("Graceful shutdown initiated...")
    em.Shutdown()
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
    em := exitmanager.Global()
    
    // Protect request processing
    if err := em.AcquireShutdownLock(); err != nil {
        http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
        return
    }
    defer em.ReleaseShutdownLock()
    
    // Simulate request processing
    time.Sleep(2 * time.Second)
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Request processed"))
}
```

## Best Practices

1. **Always pair locks**: Every `AcquireShutdownLock()` must have a corresponding `ReleaseShutdownLock()`
2. **Use defer**: Always use `defer em.ReleaseShutdownLock()` immediately after acquiring a lock
3. **Check lock acquisition**: Always check the error from `AcquireShutdownLock()`
5. **Set timeouts**: Use `SetTimeout()` to avoid hanging during process shutdown
4. **Quick cleanup**: Keep cleanup functions fast and non-blocking

## Contributing

Please report any issues or feature requests to the [GitHub repository](https://github.com/mcwalrus/exitmanager).

## License

This module is available under the MIT License.