# Integration milestones toward Theorem 1

The target is the real public implementation refining A under M. Component counts do
not measure the fraction completed. Work continues through integrated milestones;
passing another isolated helper is a checkpoint within the work.

## 1. Real request-resource accounting

- [x] Verify actual `Part` observations, `Leases` construction, physical retention,
  consecutive shared deduplication, and first-unique extraction.
- [x] Preserve a ghost record of every input part and its actual operation access.
  Prove that every logical loan is backed by a retained physical resource, even when
  several overlapping source views share one native Arc keeper.
- [x] Construct abstract member ranges/rights from those actual parts, with names indexed
  by request and member position. This supports distinct names for identical shared
  values without a runtime ID counter; actually creating fresh names is still an
  allocation/worker-state obligation in milestones 2 and 3.
- [x] Record actual data-path WQE SGEs and operation access against logical loans,
  carry complete provenance through both worker rings, and export it with owned results.
  Preserve the same resource value through staged, committed, refused, completed and
  detached states; existing side-list adapters preserve that value too. Coupling the
  entire worker's request intervals and resources to M remains milestone 3.
- [x] Handle failed MR revocation without releasing backing memory or its dependencies;
  test refusal, retry, final drop and successful cleanup with the simulated NIC.
- [x] Check erased ledger layout, model/rxe regressions and Verus. This milestone
  establishes no whole-path assembly equivalence or native-NIC performance result.

Checked integration: the actual collection loop captures every SGE and its opcode;
each logical input is covered by a recorded WQE, and every WQE SGE maps to a logical
input with an actual lifetime keeper. The projected A read/write footprint equals
the union of recorded WQE footprints with the corresponding role. `Hw::inv` keeps
the complete ledger across its real rings, and owned returns export that invariant.
Fresh logical names, byte validity and A placement remain explicit obligations.

The logical members need not correspond one-to-one with physical keepers. A shared
allocation may back several logical source loans. This requires a checked backing
relation, not an assumed arbitrary interpretation of `Leases`.

## 2. Allocation and byte permissions

Checked so far: the actual `RawMut`/`Frozen` carry the allocator's linear byte
permission through splitting, freeze/thaw and actual SGE construction. Failed MR
registration returns the whole permission for deallocation. Pool withdrawal/return
is checked given its preconditions. Actual frozen views prove bounded whole-element
clone/slice/thaw geometry and aligned pointer construction within Rust's size limit.
Request-local nonaliasing follows from the actual byte keepers, and projected groups
are well formed given distinct member names. Native `RingBuf` callers, typed validity,
cross-request/global compatibility, exposure and final cleanup remain obligations.
See `buf/memory.rs`, `buf/view.rs` and `docs/request-nonalias.md`.

Create permissions at the allocator/registration boundary and carry them in real
buffer objects through unique splitting, freezing, cloning, slicing, thawing, exposure,
revocation and release. Derive disjoint mutable access from linear permissions.
`PointsToRaw` alone does not establish initialized typed contents; keep zeroed allocation,
`FromBytes`/`IntoBytes`/`Immutable`, alignment and pointer provenance explicit.

Trusted primitives may cover the allocator, actual verbs and standard-library operations
missing from vstd, with narrow documented contracts. Do not trust the resource
bookkeeping, assume its invariant, or strengthen M merely to complete a proof.

## 3. Worker simulation and global composition

Construct the worker's A state and request/member-name maps. Couple its requests to
disjoint WQE intervals, operation-specific memory access and first-error outcomes in M.
Prove actual state transitions simulate A and M, including error and teardown paths.

Checked so far: each actual local M WQE and staged WR matches a live native request's
exact opcode/SGEs and retained byte permissions. Refused requests cannot back a reused
WQE number. The final held WQE retains its parts directly. Whole-worker outcome
aggregation, abstract request placement and global compatibility remain open.

One coherent device authority/history must support every worker. Conditional trace
lemmas over disjoint shares do not construct that authority from independent tokens.
Include worker creation/destruction, ownership transfer and observation cursors. A's
current preconditions must be established at the real global state: foreign cloning
can invalidate thaw even when local objects are unchanged.

## 4. Public guarantee and performance evidence

Connect buffer/front-end and asynchronous API behavior to the verified worker, including
backpressure, future drop/cancel, wakers and result ownership. Discharge real `Refines`;
state exactly which public operations the resulting theorem covers. Failed hardware
progress may require retaining resources, so distinguish conditional release from
unconditional leak freedom.

Then compare the intended CPU paths with native ibverbs, show ergonomic representative
applications, and use the verified substrate for higher-level libraries. Soft-RoCE is
functional evidence; native NIC measurements remain necessary for performance claims.
