void free_before_post_send_completion() {
  void* addr = malloc(length);
  struct ibv_mr* mr = ibv_reg_mr(pd, addr, length, access);

  free(addr);
	// void* new_addr = malloc(length);

	struct ibv_sge list = {
		.addr	= addr,
		.length = length,
		.lkey	= mr->lkey
	};
	struct ibv_send_wr wr = {
		.wr_id	    = PINGPONG_SEND_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
		.opcode     = IBV_WR_SEND,
		.send_flags = ctx->send_flags,
	};
  struct ibv_send_wr *bad_wr;

  // this creates an ongoing operation.
  ibv_post_send(qp, &wr, &bad_wr);
  // or free(addr) here, before completion.

  // The code can pass and actually send something to the remote.
  // But the buffer is released before completion, so the bytes sent is totally
  // undeterministic.
}