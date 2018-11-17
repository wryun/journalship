# journalship

WIP completely non-working stuff that's intended to be a simple
'I want to push journald logs to an external system'.

For some value of simple.

Things:

- CONTAINER_PARTIAL_MESSAGE
- decide on strategy for unwrapping json in MESSAGE
  (separate formatter, or add a useful function to jsone)
- kinesis protobuf encode
- kinesis shipping
- assumerole support
- shutdown logic (clean shutdown, extra channel etc.). Stager?
- clean up logging, bad error paths
- dynamically resizing worker pool so, in low-ish volume situations, we don't
  end up making too many small kinesis requests
  https://github.com/TomWright/gopool ?
  https://github.com/dc0d/workerpool ?
  https://github.com/Comdex/Octopus ?
  (is there an actually popular one?)
