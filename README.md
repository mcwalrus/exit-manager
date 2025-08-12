# Exit Manager

A Go library that provides **graceful shutdown coordination** for applications, ensuring critical operations and cleanup completes before process termination. For more information, please see the documentation at: https://pkg.go.dev/github.com/mcwalrus/exit-manager.

A Go library that provides **graceful shutdown coordination** for applications, ensuring critical operations complete before process termination. The libraries presents a simple yet powerful interface, providing cleanup handling, ensuring process exit, and managed HTTP server shutdowns. This library achieves all its functionality with zero external dependencies. For more information, please see the documentation at: https://pkg.go.dev/github.com/mcwalrus/exit-manager.

## Features

1. **🔄 Signal Handling**: Registers listener for SIGINT (Ctrl+C) and SIGTERM
2. **📡 Notifications**: When shutdown starts, the `Notify()` channel closes
3. **🔒 Lock Coordination**: Waits for all shutdown locks to be released
4. **🧹 Cleanup Execution**: Runs cleanup functions in reverse registration order  
5. **🚪 Process Exit**: Terminates with exit code 0 (success) or 1 (timeout)
6. **🌐 HTTP Server Support**: Graceful shutdown coordination for HTTP servers

## Installation

Compatible with Go 1.21+ versions:

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
1. Run the script: `go run main.go`
2. Try different timeout modes: `st 5s graceful` or `st 3s forceful`
3. Type `l` (enter) a few times to acquire locks
4. Type `s` (enter) to trigger shutdown - notice it waits for locks!
5. Type `u` (enter) to release locks and watch cleanup execute
6. Or try Ctrl+C to see real signal handling

The CLI will provide a hands-on experience that will help you understand the timeout modes and concepts before reading the code examples below.

## Testing

Test with race condition detection:

```bash
go test -race ./...
```

## API

### Timeout Protection

Prevent hanging during shutdown by setting a timeout with different modes:

```go
em := exitmanager.Global()

em.SetTimeout(exitmanager.TimeoutModeNone, 30*time.Second)
em.SetTimeout(exitmanager.TimeoutModeGraceful, 30*time.Second)
em.SetTimeout(exitmanager.TimeoutModeForceful, 30*time.Second)

// If timeout expires, the process exits with code 1
// If shutdown completes normally, the process exits with code 0
```

**Timeout Modes:**
- **`TimeoutModeNone`**: No timeout enforced, will wait indefinitely to complete shutdown
- **`TimeoutModeGraceful`**: Timeout applies only to cleanup function execution. Good for most applications
- **`TimeoutModeForceful`**: Timeout applies to the entire shutdown process. Force exit will occur even if locks are still held

### Programmatic Shutdown

Trigger shutdown from your code:

```go
em := exitmanager.Global()

go func() {
    time.Sleep(10 * time.Second)
    em.Shutdown()
}()

<-em.Notify() // Will trigger after 10 seconds
```

### Shutdown locks

Exit manager protects operations from shutdown until locks are released:

```go
em := exitmanager.Global()

if err := em.AcquireShutdownLock(); err != nil {
    // shutdown in progress, decide how to handle ...
}
defer em.ReleaseShutdownLock()

// Check active locks
if locks := em.Locks(); locks > 0 {
    log.Printf("Waiting for %d operations to complete...", locks)
}

```

### Logging

Exit manager support structured logging via `log/slog`. The logging integration provides visibility into shutdown process stages and lock operations.

```go
em := exitmanager.Global()
em.SetLogger(slog.Default(), nil)

// The shutdown process will now be communicated via default slog.Logger.
// You may need to register a flush write cleanup depending on your logger adapter for third party loggers.
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