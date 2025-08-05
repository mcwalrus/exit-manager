# Structured Logging Integration

Both exit managers now support structured logging using Go's `log/slog` package. The logging integration provides visibility into shutdown process stages, lock operations, and HTTP server management.

## Features

- **Non-pervasive design**: Logging never blocks or interferes with the shutdown process
- **No-op by default**: When no logger is configured, all logging operations are silently discarded
- **Structured output**: Uses key-value pairs for easy parsing and filtering
- **Automatic flushing**: Attempts to flush logs before process termination
- **Subsystem tagging**: All logs include `subsystem: exit-manager` for easy filtering

## Usage

### Basic Setup

```go
import (
    "log/slog"
    "os"
    
    exitmanager "github.com/mcwalrus/exit-manager"
)

func main() {
    // Create a structured logger
    logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelDebug,
    }))
    
    // Configure the exit manager with logging
    em := exitmanager.Global()
    em.SetLogger(logger)
    
    // All operations will now be logged
    em.AcquireShutdownLock()
    defer em.ReleaseShutdownLock()
    
    // ... rest of your application
}
```

### HTTP Exit Manager

The HTTP exit manager automatically receives the logger when registered:

```go
// Register HTTP exit manager (logger is automatically forwarded)
httpEM := em.RegisterHTTPExitManager()

// Register servers - operations will be logged
httpEM.RegisterHTTPServer(httpexit.HTTPServerShutdownConfig{
    Server:  server,
    Timeout: 30 * time.Second,
})
```

### Disabling Logging

```go
// Disable logging by setting to nil
em.SetLogger(nil)

// Or simply don't call SetLogger - no-op logger is used by default
```

## Log Messages

### Exit Manager Logs

| Level | Message | Keys | Description |
|-------|---------|------|-------------|
| Debug | "shutdown lock acquired" | `locks` | Lock operation completed |
| Debug | "shutdown lock released" | `locks` | Lock released, remaining count |
| Debug | "all shutdown locks released" | - | All locks cleared, proceeding with shutdown |
| Info  | "shutdown initiated" | `source`, `cleanup_functions` | Shutdown started (signal/programmatic) |
| Debug | "waiting for http exit manager shutdown" | - | Waiting for HTTP servers to shut down |
| Debug | "http exit manager shutdown completed" | - | HTTP shutdown finished |
| Debug | "waiting for shutdown locks to be released" | `locks` | Waiting for lock clearance |
| Debug | "executing cleanup functions" | `count` | Starting cleanup execution |
| Debug | "cleanup functions completed" | - | All cleanup functions finished |
| Error | "cleanup function panicked" | `panic` | Cleanup function panic (continues gracefully) |
| Info  | "graceful shutdown completed" | - | Successful shutdown |
| Warn  | "graceful shutdown timeout expired" | `timeout` | Timeout occurred |
| Warn  | "forceful shutdown timeout expired" | `timeout` | Forceful timeout occurred |

### HTTP Exit Manager Logs

| Level | Message | Keys | Description |
|-------|---------|------|-------------|
| Info  | "http exit manager registered" | - | HTTP manager registered with base manager |
| Info  | "http server registered" | `addr`, `timeout` | Server registered for shutdown |
| Info  | "http shutdown initiated" | `pre_shutdown_hooks`, `http_servers` | HTTP shutdown started |
| Debug | "executing pre-shutdown hooks" | `count` | Pre-shutdown hooks starting |
| Debug | "pre-shutdown hooks completed" | - | Pre-shutdown hooks finished |
| Debug | "shutting down http servers" | `count` | Server shutdown starting |
| Debug | "shutting down http server" | `addr` | Individual server shutdown |
| Debug | "http server shutdown completed" | `addr` | Individual server finished |
| Error | "http server shutdown error" | `addr`, `error` | Server shutdown error |
| Error | "pre-shutdown hook panicked" | `panic` | Pre-shutdown hook panic |
| Error | "http server shutdown panicked" | `panic` | Server shutdown panic |
| Debug | "all http servers shutdown completed" | - | All servers finished |
| Info  | "http shutdown completed" | - | HTTP shutdown finished |

## Log Format Example

```json
{"time":"2024-01-15T10:30:45.123Z","level":"INFO","msg":"shutdown initiated","subsystem":"exit-manager","source":"signal","cleanup_functions":2}
{"time":"2024-01-15T10:30:45.124Z","level":"DEBUG","msg":"waiting for http exit manager shutdown","subsystem":"exit-manager"}
{"time":"2024-01-15T10:30:45.125Z","level":"INFO","msg":"http shutdown initiated","subsystem":"exit-manager","component":"http","pre_shutdown_hooks":1,"http_servers":2}
{"time":"2024-01-15T10:30:45.130Z","level":"DEBUG","msg":"http server shutdown completed","subsystem":"exit-manager","component":"http","addr":":8080"}
{"time":"2024-01-15T10:30:45.135Z","level":"INFO","msg":"graceful shutdown completed","subsystem":"exit-manager"}
```

## Implementation Details

- **No-op Logger**: Uses `slog.New(slog.NewTextHandler(io.Discard, nil))` when no logger is configured
- **Structured Keys**: All logs include `subsystem: "exit-manager"`, HTTP logs add `component: "http"`
- **Flush Handling**: Attempts to call `Sync()` on handlers that support it before process exit
- **Error Resilience**: Logger failures never prevent shutdown from proceeding
- **Thread Safety**: All logging operations are thread-safe and coordination-aware