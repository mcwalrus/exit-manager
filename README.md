# Exit Manager

A Go library that provides **graceful shutdown coordination** for applications, ensuring critical operations complete before process termination. You can additionally manage cleanup and ensuring process exit. For more information, please see the documentation at: https://pkg.go.dev/github.com/mcwalrus/exit-manager.

## Features

1. **🔄 Signal Handling**: Registers listener for SIGINT (Ctrl+C) and SIGTERM
2. **📡 Notifications**: When shutdown starts, the `Notify()` channel closes
3. **🔒 Lock Coordination**: Waits for all shutdown locks to be released
4. **🧹 Cleanup Execution**: Runs cleanup functions in reverse registration order  
5. **🚪 Process Exit**: Terminates with exit code 0 (success) or 1 (timeout)

## Installation

```bash
go get github.com/mcwarlus/exit-manager
```

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
        log.Println("Closing database connections")
        database.Close()
    })

    em.RegisterCleanup(func() {
        log.Println("Close cache connections") 
        cache.Close()
    })

    em.RegisterCleanup(func() {
        log.Println("All resources cleaned up!")
    })

    // Do some work
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

## Global Access

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

## Contributing

Report issues and feature requests at the [GitHub repository](https://github.com/mcwalrus/exitmanager).

In future I would look to provide a seperate sub-module for registering http.Server's.

I am open to ideas of other integrations this library can provide.

## License

This module is available under the MIT License.