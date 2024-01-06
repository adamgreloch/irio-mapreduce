# Visions

## New process spin-up

1. Master decides to spin up a new process. It requests GCE to run a new
   machine.
2. Some GCE service boots up the process in the *null-type*.
3. A *null-type* process calls AssignConfig on a Master, who then returns
   with a configuration for that process.
4. A *null-type* process morphs into a process specified in the given config.

## Figuring where to send requests

Suppose you're a batch manager A. How do you know whether any task managers are
online and where should the doTask request be sent?

* A list of online processes doesn't change that often. It can be stored
in a stable storage.