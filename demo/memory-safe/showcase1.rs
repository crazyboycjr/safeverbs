use safeverbs::{MemoryRegion, MemorySegment, RO, SendRequest, ibv_wc};

fn free_before_post_send_completion_will_not_compile() {
    let mr: MemoryRegion<u8> = pd.allocate(size, access);

    let mut ms = mr.get_readwrite(..).unwrap();
    ms.as_mut().copy_from_slice(MSG.as_bytes());
    let ms: MemorySegment<u8, RO> = ms.freeze();
    let wr: SendRequest = qp.post_send(&ms, 1)?;

    // Drop will be delayed because wr increases the reference count of ms (and mr).
    drop(mr);

    let mut wr = wr.fuse();
    let wc: ibv_wc = futures::block_on(wr);
}