# High performance

What makes RDMA fast is posting many work requests per doorbell and asking
for a completion only now and then. In C that means keeping track, by hand,
of which buffers the unsignaled requests still use. In SafeVerbs it is safe
API:

- `qp.batch(|b| ...)` posts many SENDs and WRITEs with one doorbell and one
  completion;
- `qp.sink()` streams WRITEs and signals only as often as the send queue
  needs;
- a frozen `Buf` can be posted any number of times at once, with no check.

See `examples/perftest` (`safeverbs_write_bw`) and `examples/uchan`.
