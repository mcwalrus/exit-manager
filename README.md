# Exit Manager

[![Go Version](https://img.shields.io/github/go-mod/go-version/mcwalrus/exit-manager)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/mcwalrus/exit-manager)](https://goreportcard.com/report/github.com/mcwalrus/exit-manager)
[![codecov](https://codecov.io/gh/mcwalrus/exit-manager/branch/main/graph/badge.svg)](https://codecov.io/gh/mcwalrus/exit-manager) 
[![GoDoc](https://godoc.org/github.com/mcwalrus/exit-manager?status.svg)](https://godoc.org/github.com/mcwalrus/exit-manager)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Exit Manager is a module for **graceful shutdown coordination** to ensure critical operations complete and cleanup runs before your application exits. See [full documentation](https://pkg.go.dev/github.com/mcwalrus/exit-manager).

## Features

- **Signal handling**: Listens for SIGINT (Ctrl+C) and SIGTERM
- **Notifications**: `Notify()` channel closes when shutdown starts
- **Shutdown locks**: Prevents exit until critical operations finish
- **Cleanup functions**: Runs cleanup in reverse registration order
- **Context cancellation**: Cancels contexts when shutdown begins

## Installation

```bash
go get github.com/mcwalrus/exit-manager
```

## Quick Start

### 1. Stop Goroutines on Shutdown

Listen for shutdown in your goroutines:

```go
package main

import (
    "log"
    "time"
    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    go worker(em)
    
    <-em.Notify()
    log.Println("Shutting down...")
    select {}
}

func worker(em *exitmanager.ExitManager) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-em.Notify():
            log.Println("Worker stopping")
            return
        case <-ticker.C:
            log.Println("Working...")
        }
    }
}
```

### 2. Wait for Critical Operations

Use locks to ensure operations complete before shutdown:

```go
package main

import (
    "log"
    "time"
    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    go processData(em)
    
    log.Println("Running... Press Ctrl+C")
    <-em.Notify()

    log.Println("Waiting for operations to complete...")
    select {}
}

// performs critical operation
func processData(em *exitmanager.ExitManager) {
    // exit-manager will wait for process to complete
    _ = em.WithShutdownLock(func() error {
        time.Sleep(10 * time.Second)
        log.Println("Data processed")
    })
}
```

### 3. Clean Up Resources

Register cleanup functions that run automatically:

```go
package main

import (
    "log"
    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    
    em.RegisterCleanup(func() {
        log.Println("Closing database...")
    })
    em.RegisterCleanup(func() {
        log.Println("Closing cache...")
    })
    
    log.Println("Running... Press Ctrl+C")
    <-em.Notify()
    select {}
}
```

### 4. Context Cancellations

Use contexts that cancel on shutdown:

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
    ctx, cancel := em.NotifyContext(context.Background())
    defer cancel()
    
    go longTask(ctx)
    
    <-em.Notify()
    select {}
}

func longTask(ctx context.Context) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            log.Println("Task cancelled")
            return
        case <-ticker.C:
            log.Println("Processing...")
        }
    }
}
```

### 5. Timeout Configuration

Set a timeout to prevent shutdown from hanging:

```go
package main

import (
    "log"
    "time"
    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    em.SetTimeout(exitmanager.TimeoutModeGraceful, 10*time.Second)
    
    em.RegisterCleanup(func() {
        log.Println("Cleaning up...")
        time.Sleep(120 * time.Second) // timeout occurs first ...
    })
    
    log.Println("Running... Press Ctrl+C")
    <-em.Notify()
    select {}
}
```

### 6. Multiple Signals Handling

Configure how additional shutdown signals are handled:

```go
package main

import (
    "log"
    "time"
    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    em := exitmanager.Global()
    em.SetMultipleSignals(exitmanager.MultipleSignalsModeForcefulExit)

    em.RegisterCleanup(func() {
        log.Println("Cleaning up...")
        time.Sleep(30 * time.Second)
    })
    
    log.Println("Running... Press Ctrl+C twice for immediate exit")
    <-em.Notify()
    select {}
}
```

## Try the CLI Demo

See graceful shutdown in action:

```bash
cd cmd/exit-manager && go run .
```

## Testing

```bash
go test -race ./...
```

## Contributing

Report issues and feature requests at the [GitHub repository](https://github.com/mcwalrus/exit-manager).

## License

This module is available under the MIT License.
