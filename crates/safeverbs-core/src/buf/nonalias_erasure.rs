//! Concrete native probes for the actual request collector's proof bridges.
//!
//! Emit this crate's optimized test assembly and inspect these unmangled symbols.
//! The bridge and traversal must return immediately; the checked collector must
//! match the retention-only collector, which omits the separation traversal.

use super::{Lease, Leases, Part};
use crate::model::Model;
use crate::pd::Context;

#[unsafe(no_mangle)]
#[inline(never)]
pub fn request_nonalias_lease_bridge(left: &mut Lease<Model>, right: &mut Lease<Model>) {
    left.disjoint(right);
}

#[unsafe(no_mangle)]
#[inline(never)]
pub fn request_nonalias_keeper_traversal(leases: &mut Leases<Model>, incoming: &mut Lease<Model>) {
    leases.disjoint_incoming(incoming);
}

#[unsafe(no_mangle)]
#[inline(never)]
pub fn request_nonalias_collect_checked(leases: &mut Leases<Model>, part: Part<Model>) {
    leases.push(part);
}

#[unsafe(no_mangle)]
#[inline(never)]
pub fn request_nonalias_collect_retention_only(leases: &mut Leases<Model>, part: Part<Model>) {
    leases.push_resource(part.lease);
}

#[test]
fn split_unique_and_overlapping_shared_inputs_retain_their_actual_owners() {
    let model = Model::new(108);
    let pd = Context::new(model.clone()).alloc_pd().unwrap();
    let mut allocation = pd.alloc::<u8>(64).unwrap();
    for (i, value) in allocation.iter_mut().enumerate() {
        *value = i as u8;
    }
    let (unique, shared) = allocation.split_at(16);
    let address = unique.as_ptr();
    let shared = shared.freeze();
    let mut leases = Leases::new();
    leases.push(shared.slice(..24).into_part());
    leases.push(shared.slice(8..32).into_part());
    leases.push(unique.into_part());
    leases.push(shared.slice(16..40).into_part());
    leases.push(shared.slice(24..48).into_part());
    assert_eq!(leases.len(), 3);
    let unique = leases.into_buf_mut::<u8>().unwrap();
    assert_eq!(unique.as_ptr(), address);
    assert_eq!(&unique[..], &(0..16u8).collect::<Vec<_>>()[..]);
    let shared = shared.try_thaw().unwrap();
    assert_eq!(&shared[..], &(16..64u8).collect::<Vec<_>>()[..]);
    drop(unique);
    assert_eq!(model.registered(), 1);
    drop(shared);
    assert_eq!(model.registered(), 0);
}
