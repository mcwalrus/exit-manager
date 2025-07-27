# Exit Manager

`exitmanager` is a Go library that provides graceful shutdown coordination for Go applications, ensuring critical operations complete before process termination.

## Key Features

- 🛡️ **Safe Shutdown**: Prevents shutdown during critical operations using locks
- 🔄 **Signal Handling**: Automatically listens for SIGINT/SIGTERM signals
- 🧹 **Cleanup Coordination**: Executes cleanup functions in reverse registration order
- ⏱️ **Timeout Support**: Configurable forced exit after timeout
- 📡 **Shutdown Notifications**: Notify goroutines when shutdown begins
- 🎯 **Context Integration**: Automatic context cancellation on shutdown
- 🏗️ **Singleton Pattern**: Global access from anywhere in your application

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
    
    em.Shutdown()
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

## API Reference

### Global Access

#### `Global() *ExitManager`
Returns the singleton ExitManager instance. Creates and initializes on first call.

### Shutdown Control

#### `Shutdown()`
Programmatically initiates shutdown. Safe to call multiple times.

#### `SetTimeout(timeout time.Duration)`
Sets maximum time to wait during shutdown before forced exit. Use `<= 0` for no timeout.

### Lock Management

#### `AcquireShutdownLock() error`
Acquires a shutdown lock to protect critical operations. Returns error if shutdown already initiated.

#### `ReleaseShutdownLock()`
Releases a shutdown lock. Must be called exactly once per successful acquire.

#### `Locks() int`
Returns current number of active shutdown locks.

### Notifications

#### `Notify() <-chan struct{}`
Returns a channel that closes when shutdown is initiated.

#### `WithCancel(ctx context.Context) (context.Context, context.CancelFunc)`
Returns a context that cancels automatically when shutdown begins.

### Cleanup

#### `RegisterCleanup(f func())`
Registers a cleanup function to execute during shutdown. Functions execute in LIFO order.

## Best Practices

1. **Always pair locks**: Every `AcquireShutdownLock()` must have a corresponding `ReleaseShutdownLock()`
2. **Use defer**: Always use `defer em.ReleaseShutdownLock()` immediately after acquiring a lock
3. **Check lock acquisition**: Always check the error from `AcquireShutdownLock()`
4. **Quick cleanup**: Keep cleanup functions fast and non-blocking
5. **Set timeouts**: Use `SetTimeout()` to prevent hanging during shutdown
6. **Monitor locks**: Use `Locks()` for debugging shutdown issues

## Error Handling

The exit manager handles several error conditions gracefully:

- **Double shutdown**: Multiple calls to `Shutdown()` are safe
- **Lock after shutdown**: `AcquireShutdownLock()` returns error if shutdown initiated
- **Unmatched releases**: Extra calls to `ReleaseShutdownLock()` are safe (but indicate bugs)
- **Timeout exceeded**: Process exits with code 1 if timeout expires

## Contributing

Please report any issues or feature requests to the [GitHub repository](https://github.com/mcwalrus/exitmanager).

## License

This module is available under the MIT License.