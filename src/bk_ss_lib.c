#include <bk_ss_lib.h>

int bk_init(int* argc_ptr, char*** argv_ptr) {
	int thrd_prov, ret = MPI_SUCCESS;
	ucs_status_t stat = UCS_OK;

	// init MPI 
	ret = MPI_Init_thread(argc_ptr, argv_ptr, MPI_THREAD_MULTIPLE, &thrd_prov);
	ret = MPI_Comm_rank(MPI_COMM_WORLD, &bk_config.mpi_rank);
	BK_MPI_CHK(ret, bk_abort_init);
	ret = MPI_Comm_size(MPI_COMM_WORLD, &bk_config.mpi_size);
	BK_MPI_CHK(ret, bk_abort_init);

	// init UCP
	ucp_params_t ucp_params = {
		.field_mask = UCP_PARAM_FIELD_FEATURES |
					  UCP_PARAM_FIELD_REQUEST_SIZE |
					  UCP_PARAM_FIELD_REQUEST_INIT,
		.features = UCP_FEATURE_AMO64 | UCP_FEATURE_RMA,
		.request_size = sizeof(bk_req_t),
		.request_init = bk_req_init,
	};
	stat = ucp_init(&ucp_params, NULL, &bk_config.ucp_context);
	BK_UCS_CHK(stat, bk_abort_init);

	// create UCP worker
	ucp_worker_params_t worker_params = {
		.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
		.thread_mode = UCS_THREAD_MODE_MULTI,
	};

	stat = ucp_worker_create(bk_config.ucp_context, &worker_params, &bk_config.ucp_worker);
	BK_UCS_CHK(stat, bk_abort_init);

	stat = ucp_worker_get_address(bk_config.ucp_worker, &bk_config.ucp_address, &bk_config.ucp_address_len);
	BK_UCS_CHK(stat, bk_abort_init);
bk_abort_init:
	if (UCS_OK != stat)
		ret = MPI_ERR_OTHER;
	return ret;
}

int bk_syncroot_setup_mem() {
	int ret = MPI_SUCCESS;
	ucs_status_t stat = UCS_OK;
	bk_local_ss_t* loc_ss = &bk_config.loc_ss;

	loc_ss->counter_buf_size = sizeof(uint64_t);
	loc_ss->arr_buf_size = sizeof(uint64_t) * bk_config.mpi_size;
	ucp_mem_map_params_t mem_map_params = {
		.field_mask = UCP_MEM_MAP_PARAM_FIELD_FLAGS |
			UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
			UCP_MEM_MAP_PARAM_FIELD_LENGTH,
		.address = NULL,
		.flags = UCP_MEM_MAP_ALLOCATE,
	};

	mem_map_params.length = loc_ss->counter_buf_size;
	stat = ucp_mem_map(bk_config.ucp_context, &mem_map_params, &loc_ss->counter_mem_h);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);

	mem_map_params.length = loc_ss->arr_buf_size;
	stat = ucp_mem_map(bk_config.ucp_context, &mem_map_params, &loc_ss->arr_mem_h);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);

	ucp_mem_attr_t mem_attr = { .field_mask = UCP_MEM_ATTR_FIELD_ADDRESS };
	stat = ucp_mem_query(loc_ss->counter_mem_h, &mem_attr);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);
	loc_ss->counter_addr = (uint64_t)mem_attr.address;
	stat = ucp_mem_query(loc_ss->arr_mem_h, &mem_attr);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);
	loc_ss->arr_addr = (uint64_t)mem_attr.address;

	stat = ucp_rkey_pack(bk_config.ucp_context, loc_ss->counter_mem_h, &loc_ss->packed_counter_rkey, &loc_ss->packed_counter_rkey_size);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);
	stat = ucp_rkey_pack(bk_config.ucp_context, loc_ss->arr_mem_h, &loc_ss->packed_arr_rkey, &loc_ss->packed_arr_rkey_size);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);

	int64_t* cnt = (int64_t*)loc_ss->counter_addr;
	*cnt = -1;

bk_abort_ss_setup_mem:
	if (UCS_OK != stat)
		ret = MPI_ERR_UNKNOWN;
	return ret;
}

void bk_syncroot_destroy_mem() {
	bk_local_ss_t* loc_ss = &bk_config.loc_ss;
	ucp_rkey_buffer_release(loc_ss->packed_arr_rkey);
	ucp_rkey_buffer_release(loc_ss->packed_counter_rkey);
	ucp_mem_unmap(bk_config.ucp_context, loc_ss->counter_mem_h);
	ucp_mem_unmap(bk_config.ucp_context, loc_ss->arr_mem_h);
}

int bk_wireup_ucp() {
	ucs_status_t stat = UCS_OK;
	int ret = MPI_SUCCESS;

	// distribute rank 0 address and establish endpoint
	ucp_address_t* remote_addr = (0 == bk_config.mpi_rank) ? bk_config.ucp_address : malloc(bk_config.ucp_address_len);
	ret = MPI_Bcast(remote_addr, bk_config.ucp_address_len, MPI_BYTE, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);

	ucp_ep_params_t ep_params = {
		.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
		.address = remote_addr
	};
	stat = ucp_ep_create(bk_config.ucp_worker, &ep_params, &bk_config.ucp_ep);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	if (0 == bk_config.mpi_rank) {
		bk_syncroot_setup_mem();
	}

	ret = MPI_Bcast(&bk_config.loc_ss.packed_counter_rkey_size, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);
	size_t packed_buf_size = bk_config.loc_ss.packed_counter_rkey_size;
	void* tmp_packed_rkey_buf = malloc(packed_buf_size);

	if (0 == bk_config.mpi_rank)memcpy(tmp_packed_rkey_buf, bk_config.loc_ss.packed_counter_rkey, packed_buf_size);
	ret = MPI_Bcast(tmp_packed_rkey_buf, packed_buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);
	stat = ucp_ep_rkey_unpack(bk_config.ucp_ep, tmp_packed_rkey_buf, &bk_config.remote_ss.counter_rkey);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	if (0 == bk_config.mpi_rank)memcpy(tmp_packed_rkey_buf, bk_config.loc_ss.packed_arr_rkey, packed_buf_size);
	ret = MPI_Bcast(tmp_packed_rkey_buf, packed_buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);
	stat = ucp_ep_rkey_unpack(bk_config.ucp_ep, tmp_packed_rkey_buf, &bk_config.remote_ss.arr_rkey);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	if (0 == bk_config.mpi_rank)bk_config.remote_ss.counter_addr = bk_config.loc_ss.counter_addr;
	ret = MPI_Bcast(&bk_config.remote_ss.counter_addr, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);

	if (0 == bk_config.mpi_rank)bk_config.remote_ss.arr_addr = bk_config.loc_ss.arr_addr;
	ret = MPI_Bcast(&bk_config.remote_ss.arr_addr, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);

	// exchange UCP endpoints
bk_abort_wireup_ucp:
	if (0 == bk_config.mpi_rank)free(remote_addr);
	if (UCS_OK != stat)ret = MPI_ERR_UNKNOWN;
	return ret;
}


int bk_finalize() {
	ucs_status_ptr_t stat_ptr = UCS_OK;
	if (0 == bk_config.mpi_rank) {
		bk_syncroot_destroy_mem();
	}

	ucp_rkey_destroy(bk_config.remote_ss.arr_rkey);
	ucp_rkey_destroy(bk_config.remote_ss.counter_rkey);

	stat_ptr = ucp_ep_close_nb(bk_config.ucp_ep, UCP_EP_CLOSE_MODE_FORCE);
	while (UCS_INPROGRESS == UCS_PTR_STATUS(stat_ptr))ucp_worker_progress(bk_config.ucp_worker);
	MPI_Barrier(MPI_COMM_WORLD);
	// ucp_worker_release_address(bk_config.ucp_worker, bk_config.ucp_address);
	ucp_worker_destroy(bk_config.ucp_worker);
	ucp_cleanup(bk_config.ucp_context);

	MPI_Finalize();

	return MPI_SUCCESS;
}

void bk_req_init(void* req) {
	bk_req_t* r = req;
	r->status = UCS_OK;
	r->completed = 0;
}

void _bk_send_cb(void* request, ucs_status_t status, void* args) {
	bk_req_t* req = request;
	req->status = status;
	req->completed = 1;
}

inline ucs_status_t _bk_poll_completion(ucs_status_ptr_t status_ptr) {
	if (UCS_OK == status_ptr) {
		return UCS_OK;
	}

	if (UCS_PTR_IS_ERR(status_ptr)) {
		BK_ERROR("poll completion returing error %d (%s)", UCS_PTR_STATUS(status_ptr), ucs_status_string(UCS_PTR_STATUS(status_ptr)));
		ucp_request_free(status_ptr);
		return UCS_PTR_STATUS(status_ptr);
	}
	ucs_status_t status = UCS_OK;
	bk_req_t* req = status_ptr;

	while (UCS_INPROGRESS != UCS_PTR_STATUS(status_ptr) || !req->completed) {
		ucp_worker_progress(bk_config.ucp_worker);
	}

	status = req->status;
	req->completed = 0;
	req->status = UCS_INPROGRESS;
	ucp_request_free(status_ptr);
	return status;
}

int bk_reset_local() {
	if (0 != bk_config.mpi_rank) {
		BK_ERROR("Only rank 0 can call reset_local");
		return MPI_ERR_RANK;
	}
	bk_local_ss_t* loc_ss = &bk_config.loc_ss;
	int64_t* cnt = (int64_t*)loc_ss->counter_addr, * arr = (int64_t*)loc_ss->arr_addr;
	*cnt = -1;
	for(int i = 0; i<bk_config.mpi_size; i++){
		arr[i] = -1;
	}
	return MPI_SUCCESS;
}