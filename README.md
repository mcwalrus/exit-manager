# exit-manager

WIP!

Exit manager is a Go module for handling `SIGINT` and `SIGTERM` signals gracefully in long-running services. It gives you control over when and how your application shuts down, allowing an application to complete and exit cleanly across multiple routines. Additionally, the exit manager provides methods to block future requests while the application is closing down.


## Use Cases

* Handling graceful shutdown across distributed or batched processing systems.
* Notifying shutdown signals to multiple goroutines for cleanup.
* Allowing in-flight requests to complete before terminating.

## Features

You must register the exit manager to effect the shutdown process. Actions to be taken when `SIGINT` or `SIGTERM` is recieved:

| State           | Action Taken                                    |
| --------------- | ----------------------------------------------- |
| Lock not held   | Immediate exit with `os.Exit(0)`                |
| Lock held       | Waits for all locks to be released              |
| Timeout reached | Force exit with `os.Exit(1)`                    |
| All done early  | Clean exit with `os.Exit(0)`                    |

## CLI Demo Tool

Explore `/cli` to try the exit manager in different use cases interactively. The program uses keybindings to trigger signals, lock/unlock the exit manager, and simulate real-world shutdown behavior. Useful for learning, testing, and debugging.

## Future

- [ ] Register logging adapter pattern for external loggers such as zerolog, etc.
