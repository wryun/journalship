# journalship

WIP completely non-working stuff that's intended to be a simple
'I want to push journald logs to an external system'.

For some value of simple.

Things:

- field selection (rather than many call enumerate)
- cursor load/chunk shipped reporting (double counts, list, etc.)
- CONTAINER_PARTIAL_MESSAGE
- decide on strategy for unwrapping json in MESSAGE
  (separate formatter, or add a useful function to jsone)
- kinesis protobuf encode
- kinesis shipping
- assumerole support
- shutdown logic (clean shutdown, extra channel etc.). Stager?
- clean up logging, bad error paths
