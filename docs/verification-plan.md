# Integration milestones toward Theorem 1

The target is the real public implementation refining A under M. Component counts do
not measure the fraction completed. Work continues through integrated milestones;
passing another isolated helper is a checkpoint within the work.

## 1. Real request-resource accounting

- [x] Verify actual `Part` observations, `Leases` construction, physical retention,
  consecutive shared deduplication, and first-unique extraction.
- [ ] Preserve a ghost record of every input part and its actual operation access.
  Prove that every logical loan is backed by a retained physical resource, even when
  several overlapping source views share one native Arc keeper.
- [ ] Construct abstract member ranges/rights from those actual parts, with names indexed
  by request and member position. Identical shared values in different requests must
  receive distinct logical names without a runtime ID counter.
- [ ] Relate every posted WQE's SGEs to the recorded loans, carry the relation through
  the ring, and return the same ledger with its result. Keep staged, committed,
  refused, completed, detached and side-list states distinct.
- [ ] Handle failed MR revocation without releasing backing memory or its dependencies;
  test refusal, retry, final drop and successful cleanup with the simulated NIC.
- [ ] Check erasure/layout, relevant optimized code, model/rxe regressions and Verus.

The logical members need not correspond one-to-one with physical keepers. A shared
allocation may back several logical source loans. This requires a checked backing
relation, not an assumed arbitrary interpretation of `Leases`.

## 2. Allocation and byte permissions

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
