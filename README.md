# journalship

WIP stuff that's intended to be a simple 'I want to push journald
logs to an external system'.

For some value of simple.

## MVP

- test kinesis shipping/assumerole/protobuf
- generic retry/backoff support (?)

## Not fixing

- CONTAINER_PARTIAL_MESSAGE not correctly updating cursor
- https://github.com/moby/moby/issues/38045 (>18.03 ... fixed at 18.10?)

## Quality

- unit tests
- e2e tests
- prometheus metrics endpoint (in particular, for throttling/retry/whatever)
- shutdown logic (clean shutdown, extra channel etc.). Stager?
- clean up logging, bad error paths
- consider what the ideal logging situation should be
  (demand a file, to avoid loops?)
- dynamically resizing worker pool so, in low-ish volume situations, we don't
  end up making too many small kinesis requests
  https://github.com/TomWright/gopool ?
  https://github.com/dc0d/workerpool ?
  https://github.com/Comdex/Octopus ?
  (is there an actually popular one?)
