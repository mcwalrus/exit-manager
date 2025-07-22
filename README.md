# Go Exit Manager

Go exit manager is a library to manage `SIGINT` requests gracefully for services. Sometimes we might want delay our exit around particular routinues, you can use this library to lock, and set sensible timeout expecatations as to when the service should be allowed to exit immediately, and notifying routinues to exit gracefully.

## Cases

Examples for when an exit manager might apply for you:

* Processing multiple requests across distributed systems in sync.
* Wanting batch processing routinues to complete in an atomic fashion.
* Notifying multiple channels and routinues when `SIGINT` is recieved.

## Purpose

The desired functionality is that:

* Register the exit manager with delay configuration.
* On reciving `SIGINT`, the exit manager checks the exit lock is held.
* If held, the exit manager will notify any routinues that `SIGINT` was recieved.
* The exit manager will wait the duration of the delay timeout before calling `os.Exit(1)`.
* Otherwise if all registered routinues complete first, the exit manager will instead call `os.Exit(0)`.
* Default behaviour is to call `os.Exit(0)` immediately when the exit lock is not held.


## Try it out

Under `/cli/exit-manager` you can run the exit manager locally to learn the exit control manually. Timeouts can be set through the cli arguments, where `SIGTERM`, `SIGINT` signals as well as the exit manager's locking control mechanisms can be controlled through key-bindings. This can allow you to play trial with the manager to fully understand the control before you use it in your code.

## Future

- [ ] Manage system cleanup.
- [ ] Provide tests which hijack / replace `os.SIGTERM`, `os.SIGINT`.
- [ ] Register logging adapter pattern for external loggers such as zerolog, etc.
- [ ] Reword goal to be positive case first.
