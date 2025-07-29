# Exit Manager

`exitmanager` is a Go library that provides graceful shutdown coordination for applications, ensuring critical operations complete before process termination.

## Key Features

- 🛡️ **Safe Shutdown**: Prevents shutdown during critical operations using locks
- 🔄 **Signal Handling**: Automatically listens for SIGINT/SIGTERM or shutdown signals
- 🎯 **Context Integration**: Register cancellation contexts on notified shutdown
- 🧹 **Cleanup Coordination**: Executes cleanup functions in reverse registration order
- 📡 **Shutdown Notifications**: Notify goroutines when notified shutdown begins
- ⏱️ **Timeout Support**: Configurable forced exit after timeout

## Installation

```bash
go get github.com/mcwarlus/exit-manager
```

## When to Use?

**1) Critical Process Management**

Use exit-manager when you have multiple goroutines performing critical work that must complete before the application shuts down. The lock system ensures no operations are interrupted mid-process unless a timeout is set.

**2) Resource Cleanup**

Use exit-manager when you need to guarantee or manage cleanup functions execute in a specific order during shutdown (database connections, file handles, network connections, etc).

## Best Practices

1. **Always pair locks**: Every `AcquireShutdownLock()` must have a corresponding `ReleaseShutdownLock()`
2. **Use defer**: Always use `defer em.ReleaseShutdownLock()` immediately after acquiring a lock
3. **Check lock acquisition**: Always check the error from `AcquireShutdownLock()`
5. **Set timeouts**: Use `SetTimeout()` to avoid hanging during process shutdown
4. **Quick cleanup**: Keep cleanup functions fast and non-blocking

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

    // Set a timeout for forced shutdown
    em.SetTimeout(30 * time.Second)

    // Shutdown process after sleep
    go func () {
        time.Sleep(10 * time.Second)
        em.Shutdown()
    }()

    for {
        // Protect critical operation
        if err := em.AcquireShutdownLock(); err == nil {
            log.Printf("Worker: shutdown in progress, exiting", id)
            
        }
        // Simulate work release lock
        log.Printf("Worker: processing...", id)
        time.Sleep(2 * time.Second)
        em.ReleaseShutdownLock()
        
        // Check for notified shutdown
        select {
        case <-em.Notify():
            log.Printf("Worker: received shutdown signal", id)
            break
        default:
            // Continue working
        }
    }
    
    // Wait forever, exit-manager to exit process
    select {}
}
```

### Acquiring Shutdown Locks

You can acquire multiple shutdown locks at once for critical operations.

```go
package main

import (
    "log"
    "time"
    "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()

    // Shutdown process after sleep
    go func () {
        time.Sleep(10 * time.Second)
        em.Shutdown()
    }()

    // Start worker goroutines
    for i := 0; i < 3; i++ {
        go doWork(i)
    }

    // Wait for shutdown signal
    <-em.Notify()
    log.Println("Shutdown initiated, waiting for workers...")
    
    // Wait forever, exit-manager to exit process
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
        
        // Simulate work, release lock
        log.Printf("Worker %d: processing...", id)
        time.Sleep(2.34 * time.Second)
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

You can register contexts with the exit-manager to be cancelled on notified shutdown.

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

    // Cleanup avoids immediate exit
    em.RegisterCleanup(func() {
        time.Sleep(5 * time.Second)
    })

    // Sleep then signal for shutdown
    time.Sleep(10 * time.Second)
    em.Shutdown()

    // Wait for manager to exit process 
    select {}
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

## Test

To test the library, please use the race condition checker:

```
$ go test -race .
```

## Contributing

Please report any issues or feature requests to the [GitHub repository](https://github.com/mcwalrus/exitmanager).

## License

This module is available under the MIT License.