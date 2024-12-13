/// This file is adapted from rdma-core/libibverbs/examples/rc_pingpong.c.

static int pp_post_recv(struct pingpong_context *ctx, int n)
{
	struct ibv_sge list = {
		.addr	= use_dm ? 0 : (uintptr_t) ctx->buf,
		.length = ctx->size,
		.lkey	= ctx->mr->lkey
	};
	struct ibv_recv_wr wr = {
		.wr_id	    = PINGPONG_RECV_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
	};
	struct ibv_recv_wr *bad_wr;
	int i;

	for (i = 0; i < n; ++i)
		if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
			break;

	return i;
}

static int pp_post_send(struct pingpong_context *ctx)
{
	struct ibv_sge list = {
		.addr	= use_dm ? 0 : (uintptr_t) ctx->buf,
		.length = ctx->size,
		.lkey	= ctx->mr->lkey
	};
	struct ibv_send_wr wr = {
		.wr_id	    = PINGPONG_SEND_WRID,
		.sg_list    = &list,
		.num_sge    = 1,
		.opcode     = IBV_WR_SEND,
		.send_flags = ctx->send_flags,
	};
	struct ibv_send_wr *bad_wr;

	if (use_new_send) {
		ibv_wr_start(ctx->qpx);

		ctx->qpx->wr_id = PINGPONG_SEND_WRID;
		ctx->qpx->wr_flags = ctx->send_flags;

		ibv_wr_send(ctx->qpx);
		ibv_wr_set_sge(ctx->qpx, list.lkey, list.addr, list.length);

		return ibv_wr_complete(ctx->qpx);
	} else {
		return ibv_post_send(ctx->qp, &wr, &bad_wr);
	}
}

struct ts_params {
	uint64_t		 comp_recv_max_time_delta;
	uint64_t		 comp_recv_min_time_delta;
	uint64_t		 comp_recv_total_time_delta;
	uint64_t		 comp_recv_prev_time;
	int			 last_comp_with_ts;
	unsigned int		 comp_with_time_iters;
};

static inline int parse_single_wc(struct pingpong_context *ctx, int *scnt,
				  int *rcnt, int *routs, int iters,
				  uint64_t wr_id, enum ibv_wc_status status,
				  uint64_t completion_timestamp,
				  struct ts_params *ts)
{
	if (status != IBV_WC_SUCCESS) {
		fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
			ibv_wc_status_str(status),
			status, (int)wr_id);
		return 1;
	}

	switch ((int)wr_id) {
	case PINGPONG_SEND_WRID:
		++(*scnt);
		break;

	case PINGPONG_RECV_WRID:
		if (--(*routs) <= 1) {
			*routs += pp_post_recv(ctx, ctx->rx_depth - *routs);
			if (*routs < ctx->rx_depth) {
				fprintf(stderr,
					"Couldn't post receive (%d)\n",
					*routs);
				return 1;
			}
		}

		++(*rcnt);
		if (use_ts) {
			if (ts->last_comp_with_ts) {
				uint64_t delta;

				/* checking whether the clock was wrapped around */
				if (completion_timestamp >= ts->comp_recv_prev_time)
					delta = completion_timestamp - ts->comp_recv_prev_time;
				else
					delta = ctx->completion_timestamp_mask - ts->comp_recv_prev_time +
						completion_timestamp + 1;

				ts->comp_recv_max_time_delta = max(ts->comp_recv_max_time_delta, delta);
				ts->comp_recv_min_time_delta = min(ts->comp_recv_min_time_delta, delta);
				ts->comp_recv_total_time_delta += delta;
				ts->comp_with_time_iters++;
			}

			ts->comp_recv_prev_time = completion_timestamp;
			ts->last_comp_with_ts = 1;
		} else {
			ts->last_comp_with_ts = 0;
		}

		break;

	default:
		fprintf(stderr, "Completion for unknown wr_id %d\n",
			(int)wr_id);
		return 1;
	}

	ctx->pending &= ~(int)wr_id;
	if (*scnt < iters && !ctx->pending) {
		if (pp_post_send(ctx)) {
			fprintf(stderr, "Couldn't post send\n");
			return 1;
		}
		ctx->pending = PINGPONG_RECV_WRID |
			PINGPONG_SEND_WRID;
	}

	return 0;
}

static void usage(const char *argv0)
{
	printf("Usage:\n");
	printf("  %s            start a server and wait for connection\n", argv0);
	printf("  %s <host>     connect to server at <host>\n", argv0);
	printf("\n");
	printf("Options:\n");
	printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
	printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
	printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
	printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
	printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
	printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
	printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
	printf("  -l, --sl=<sl>          service level value\n");
	printf("  -e, --events           sleep on CQ events (default poll)\n");
	printf("  -g, --gid-idx=<gid index> local port gid index\n");
	printf("  -o, --odp		    use on demand paging\n");
	printf("  -O, --iodp		    use implicit on demand paging\n");
	printf("  -P, --prefetch	    prefetch an ODP MR\n");
	printf("  -t, --ts	            get CQE with timestamp\n");
	printf("  -c, --chk	            validate received buffer\n");
	printf("  -j, --dm	            use device memory\n");
	printf("  -N, --new_send            use new post send WR API\n");
}

int main(int argc, char *argv[])
{
	struct ibv_device      **dev_list;
	struct ibv_device	*ib_dev;
	struct pingpong_context *ctx;
	struct pingpong_dest     my_dest;
	struct pingpong_dest    *rem_dest;
	struct timeval           start, end;
	char                    *ib_devname = NULL;
	char                    *servername = NULL;
	unsigned int             port = 18515;
	int                      ib_port = 1;
	unsigned int             size = 4096;
	enum ibv_mtu		 mtu = IBV_MTU_1024;
	unsigned int             rx_depth = 500;
	unsigned int             iters = 1000;
	int                      use_event = 0;
	int                      routs;
	int                      rcnt, scnt;
	int                      num_cq_events = 0;
	int                      sl = 0;
	int			 gidx = -1;
	char			 gid[33];
	struct ts_params	 ts;

	srand48(getpid() * time(NULL));

	while (1) {
		int c;

		static struct option long_options[] = {
			{ .name = "port",     .has_arg = 1, .val = 'p' },
			{ .name = "ib-dev",   .has_arg = 1, .val = 'd' },
			{ .name = "ib-port",  .has_arg = 1, .val = 'i' },
			{ .name = "size",     .has_arg = 1, .val = 's' },
			{ .name = "mtu",      .has_arg = 1, .val = 'm' },
			{ .name = "rx-depth", .has_arg = 1, .val = 'r' },
			{ .name = "iters",    .has_arg = 1, .val = 'n' },
			{ .name = "sl",       .has_arg = 1, .val = 'l' },
			{ .name = "events",   .has_arg = 0, .val = 'e' },
			{ .name = "gid-idx",  .has_arg = 1, .val = 'g' },
			{ .name = "odp",      .has_arg = 0, .val = 'o' },
			{ .name = "iodp",     .has_arg = 0, .val = 'O' },
			{ .name = "prefetch", .has_arg = 0, .val = 'P' },
			{ .name = "ts",       .has_arg = 0, .val = 't' },
			{ .name = "chk",      .has_arg = 0, .val = 'c' },
			{ .name = "dm",       .has_arg = 0, .val = 'j' },
			{ .name = "new_send", .has_arg = 0, .val = 'N' },
			{}
		};

		c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:eg:oOPtcjN",
				long_options, NULL);

		if (c == -1)
			break;

		switch (c) {
		case 'p':
			port = strtoul(optarg, NULL, 0);
			if (port > 65535) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'd':
			ib_devname = strdupa(optarg);
			break;

		case 'i':
			ib_port = strtol(optarg, NULL, 0);
			if (ib_port < 1) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 's':
			size = strtoul(optarg, NULL, 0);
			break;

		case 'm':
			mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
			if (mtu == 0) {
				usage(argv[0]);
				return 1;
			}
			break;

		case 'r':
			rx_depth = strtoul(optarg, NULL, 0);
			break;

		case 'n':
			iters = strtoul(optarg, NULL, 0);
			break;

		case 'l':
			sl = strtol(optarg, NULL, 0);
			break;

		case 'e':
			++use_event;
			break;

		case 'g':
			gidx = strtol(optarg, NULL, 0);
			break;

		case 'o':
			use_odp = 1;
			break;
		case 'P':
			prefetch_mr = 1;
			break;
		case 'O':
			use_odp = 1;
			implicit_odp = 1;
			break;
		case 't':
			use_ts = 1;
			break;
		case 'c':
			validate_buf = 1;
			break;

		case 'j':
			use_dm = 1;
			break;

		case 'N':
			use_new_send = 1;
			break;

		default:
			usage(argv[0]);
			return 1;
		}
	}

	if (optind == argc - 1)
		servername = strdupa(argv[optind]);
	else if (optind < argc) {
		usage(argv[0]);
		return 1;
	}

	if (use_odp && use_dm) {
		fprintf(stderr, "DM memory region can't be on demand\n");
		return 1;
	}

	if (!use_odp && prefetch_mr) {
		fprintf(stderr, "prefetch is valid only with on-demand memory region\n");
		return 1;
	}

	if (use_ts) {
		ts.comp_recv_max_time_delta = 0;
		ts.comp_recv_min_time_delta = 0xffffffff;
		ts.comp_recv_total_time_delta = 0;
		ts.comp_recv_prev_time = 0;
		ts.last_comp_with_ts = 0;
		ts.comp_with_time_iters = 0;
	}

	page_size = sysconf(_SC_PAGESIZE);

	dev_list = ibv_get_device_list(NULL);
	if (!dev_list) {
		perror("Failed to get IB devices list");
		return 1;
	}

	if (!ib_devname) {
		ib_dev = *dev_list;
		if (!ib_dev) {
			fprintf(stderr, "No IB devices found\n");
			return 1;
		}
	} else {
		int i;
		for (i = 0; dev_list[i]; ++i)
			if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
				break;
		ib_dev = dev_list[i];
		if (!ib_dev) {
			fprintf(stderr, "IB device %s not found\n", ib_devname);
			return 1;
		}
	}

	ctx = pp_init_ctx(ib_dev, size, rx_depth, ib_port, use_event);
	if (!ctx)
		return 1;

	routs = pp_post_recv(ctx, ctx->rx_depth);
	if (routs < ctx->rx_depth) {
		fprintf(stderr, "Couldn't post receive (%d)\n", routs);
		return 1;
	}

	if (use_event)
		if (ibv_req_notify_cq(pp_cq(ctx), 0)) {
			fprintf(stderr, "Couldn't request CQ notification\n");
			return 1;
		}


	if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
		fprintf(stderr, "Couldn't get port info\n");
		return 1;
	}

	my_dest.lid = ctx->portinfo.lid;
	if (ctx->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
							!my_dest.lid) {
		fprintf(stderr, "Couldn't get local LID\n");
		return 1;
	}

	if (gidx >= 0) {
		if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
			fprintf(stderr, "can't read sgid of index %d\n", gidx);
			return 1;
		}
	} else
		memset(&my_dest.gid, 0, sizeof my_dest.gid);

	my_dest.qpn = ctx->qp->qp_num;
	my_dest.psn = lrand48() & 0xffffff;
	inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
	printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	       my_dest.lid, my_dest.qpn, my_dest.psn, gid);


	if (servername)
		rem_dest = pp_client_exch_dest(servername, port, &my_dest);
	else
		rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl,
								&my_dest, gidx);

	if (!rem_dest)
		return 1;

	inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
	printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
	       rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

	if (servername)
		if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest,
					gidx))
			return 1;

	ctx->pending = PINGPONG_RECV_WRID;

	if (servername) {
		if (validate_buf)
			for (int i = 0; i < size; i += page_size)
				ctx->buf[i] = i / page_size % sizeof(char);

		if (use_dm)
			if (ibv_memcpy_to_dm(ctx->dm, 0, (void *)ctx->buf, size)) {
				fprintf(stderr, "Copy to dm buffer failed\n");
				return 1;
			}

		if (pp_post_send(ctx)) {
			fprintf(stderr, "Couldn't post send\n");
			return 1;
		}
		ctx->pending |= PINGPONG_SEND_WRID;
	}

	if (gettimeofday(&start, NULL)) {
		perror("gettimeofday");
		return 1;
	}

	rcnt = scnt = 0;
	while (rcnt < iters || scnt < iters) {
		int ret;

		if (use_event) {
			struct ibv_cq *ev_cq;
			void          *ev_ctx;

			if (ibv_get_cq_event(ctx->channel, &ev_cq, &ev_ctx)) {
				fprintf(stderr, "Failed to get cq_event\n");
				return 1;
			}

			++num_cq_events;

			if (ev_cq != pp_cq(ctx)) {
				fprintf(stderr, "CQ event for unknown CQ %p\n", ev_cq);
				return 1;
			}

			if (ibv_req_notify_cq(pp_cq(ctx), 0)) {
				fprintf(stderr, "Couldn't request CQ notification\n");
				return 1;
			}
		}

		if (use_ts) {
			struct ibv_poll_cq_attr attr = {};

			do {
				ret = ibv_start_poll(ctx->cq_s.cq_ex, &attr);
			} while (!use_event && ret == ENOENT);

			if (ret) {
				fprintf(stderr, "poll CQ failed %d\n", ret);
				return ret;
			}
			ret = parse_single_wc(ctx, &scnt, &rcnt, &routs,
					      iters,
					      ctx->cq_s.cq_ex->wr_id,
					      ctx->cq_s.cq_ex->status,
					      ibv_wc_read_completion_ts(ctx->cq_s.cq_ex),
					      &ts);
			if (ret) {
				ibv_end_poll(ctx->cq_s.cq_ex);
				return ret;
			}
			ret = ibv_next_poll(ctx->cq_s.cq_ex);
			if (!ret)
				ret = parse_single_wc(ctx, &scnt, &rcnt, &routs,
						      iters,
						      ctx->cq_s.cq_ex->wr_id,
						      ctx->cq_s.cq_ex->status,
						      ibv_wc_read_completion_ts(ctx->cq_s.cq_ex),
						      &ts);
			ibv_end_poll(ctx->cq_s.cq_ex);
			if (ret && ret != ENOENT) {
				fprintf(stderr, "poll CQ failed %d\n", ret);
				return ret;
			}
		} else {
			int ne, i;
			struct ibv_wc wc[2];

			do {
				ne = ibv_poll_cq(pp_cq(ctx), 2, wc);
				if (ne < 0) {
					fprintf(stderr, "poll CQ failed %d\n", ne);
					return 1;
				}
			} while (!use_event && ne < 1);

			for (i = 0; i < ne; ++i) {
				ret = parse_single_wc(ctx, &scnt, &rcnt, &routs,
						      iters,
						      wc[i].wr_id,
						      wc[i].status,
						      0, &ts);
				if (ret) {
					fprintf(stderr, "parse WC failed %d\n", ne);
					return 1;
				}
			}
		}
	}

	if (gettimeofday(&end, NULL)) {
		perror("gettimeofday");
		return 1;
	}

	{
		float usec = (end.tv_sec - start.tv_sec) * 1000000 +
			(end.tv_usec - start.tv_usec);
		long long bytes = (long long) size * iters * 2;

		printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n",
		       bytes, usec / 1000000., bytes * 8. / usec);
		printf("%d iters in %.2f seconds = %.2f usec/iter\n",
		       iters, usec / 1000000., usec / iters);

		if (use_ts && ts.comp_with_time_iters) {
			printf("Max receive completion clock cycles = %" PRIu64 "\n",
			       ts.comp_recv_max_time_delta);
			printf("Min receive completion clock cycles = %" PRIu64 "\n",
			       ts.comp_recv_min_time_delta);
			printf("Average receive completion clock cycles = %f\n",
			       (double)ts.comp_recv_total_time_delta / ts.comp_with_time_iters);
		}

		if ((!servername) && (validate_buf)) {
			if (use_dm)
				if (ibv_memcpy_from_dm(ctx->buf, ctx->dm, 0, size)) {
					fprintf(stderr, "Copy from DM buffer failed\n");
					return 1;
				}
			for (int i = 0; i < size; i += page_size)
				if (ctx->buf[i] != i / page_size % sizeof(char))
					printf("invalid data in page %d\n",
					       i / page_size);
		}
	}

	ibv_ack_cq_events(pp_cq(ctx), num_cq_events);

	if (pp_close_ctx(ctx))
		return 1;

	ibv_free_device_list(dev_list);
	free(rem_dest);

	return 0;
}