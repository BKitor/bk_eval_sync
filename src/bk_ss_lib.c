#include <bk_ss_lib.h>
#include <getopt.h>
#include <string.h>

bk_exp_flags_t default_exp_flags = {
	.print_help = 0,
	.num_itters = 10,
	.num_warmups = 1,
	.max_delay = 0,
	.verbosity = 0,
	.experiment = BK_EXP_UCP_ATOMIC,
};

bk_synctest_config_t default_synctest_config = {
	.ucp_context = NULL,
	.ucp_worker = NULL,
	.ucp_address = NULL,
	.ucp_address_len = -1,
	.ucp_ep = NULL,
	.mpi_cnt_win = NULL,
	.mpi_cnt_baseptr = NULL,
	.mpi_arr_win = NULL,
	.mpi_arr_baseptr = NULL,
	.loc_ucp_ss = {0},
	.remote_ucp_ss = {0},
	.loc_mpi_ss = {0},
	.remote_mpi_ss = {0},
	.exp_flags = {0},
	.mpi_rank = -1,
	.mpi_size = -1,
};

// MPI_Init
// Process command line args
// ucp_init, ucp_worker create
// setup bk_config global variable
int bk_init(int argc, char** argv, bk_synctest_config_t* cfg) {
	int thrd_prov, ret = MPI_SUCCESS;
	ucs_status_t stat = UCS_OK;
	memcpy(&cfg->exp_flags, &default_exp_flags, sizeof(cfg->exp_flags));

	// init MPI 
	ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &thrd_prov);
	ret = MPI_Comm_rank(MPI_COMM_WORLD, &cfg->mpi_rank);
	BK_MPI_CHK(ret, bk_abort_init);
	ret = MPI_Comm_size(MPI_COMM_WORLD, &cfg->mpi_size);
	BK_MPI_CHK(ret, bk_abort_init);

	ret = process_cmd_flags(argc, argv, &cfg->exp_flags);
	BK_MPI_CHK(ret, bk_abort_init);

bk_abort_init:
	if (UCS_OK != stat)
		ret = MPI_ERR_OTHER;
	return ret;
}

int bk_syncroot_setup_ucp_mem(bk_synctest_config_t* bk_cfg) {
	int ret = MPI_SUCCESS;
	ucs_status_t stat = UCS_OK;
	bk_ucp_local_ss_t* loc_ss = &bk_cfg->loc_ucp_ss;


	loc_ss->counter_buf_size = sizeof(uint64_t);
	loc_ss->arr_buf_size = sizeof(uint64_t) * bk_cfg->mpi_size;
	ucp_mem_map_params_t mem_map_params = {
		.field_mask = UCP_MEM_MAP_PARAM_FIELD_FLAGS |
			UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
			UCP_MEM_MAP_PARAM_FIELD_LENGTH,
		.address = NULL,
		.flags = UCP_MEM_MAP_ALLOCATE,
	};

	mem_map_params.length = loc_ss->counter_buf_size;
	stat = ucp_mem_map(bk_cfg->ucp_context, &mem_map_params, &loc_ss->counter_mem_h);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);

	mem_map_params.length = loc_ss->arr_buf_size;
	stat = ucp_mem_map(bk_cfg->ucp_context, &mem_map_params, &loc_ss->arr_mem_h);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);

	ucp_mem_attr_t mem_attr = { .field_mask = UCP_MEM_ATTR_FIELD_ADDRESS };
	stat = ucp_mem_query(loc_ss->counter_mem_h, &mem_attr);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);
	loc_ss->counter_addr = (uint64_t)mem_attr.address;
	stat = ucp_mem_query(loc_ss->arr_mem_h, &mem_attr);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);
	loc_ss->arr_addr = (uint64_t)mem_attr.address;

	stat = ucp_rkey_pack(bk_cfg->ucp_context, loc_ss->counter_mem_h, &loc_ss->packed_counter_rkey, &loc_ss->packed_counter_rkey_size);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);
	stat = ucp_rkey_pack(bk_cfg->ucp_context, loc_ss->arr_mem_h, &loc_ss->packed_arr_rkey, &loc_ss->packed_arr_rkey_size);
	BK_UCS_CHK(stat, bk_abort_ss_setup_mem);

	int64_t* cnt = (int64_t*)loc_ss->counter_addr;
	*cnt = -1;

bk_abort_ss_setup_mem:
	if (UCS_OK != stat)
		ret = MPI_ERR_UNKNOWN;
	return ret;
}

void bk_ucp_syncroot_destroy_mem(bk_synctest_config_t* bk_cfg) {
	bk_ucp_local_ss_t* loc_ss = &bk_cfg->loc_ucp_ss;
	ucp_rkey_buffer_release(loc_ss->packed_arr_rkey);
	ucp_rkey_buffer_release(loc_ss->packed_counter_rkey);
	ucp_mem_unmap(bk_cfg->ucp_context, loc_ss->counter_mem_h);
	ucp_mem_unmap(bk_cfg->ucp_context, loc_ss->arr_mem_h);
}

int bk_ucp_wireup(bk_synctest_config_t* bk_cfg) {
	ucs_status_t stat = UCS_OK;
	int ret = MPI_SUCCESS;

	// init UCP
	ucp_params_t ucp_params = {
		.field_mask = UCP_PARAM_FIELD_FEATURES |
					  UCP_PARAM_FIELD_REQUEST_SIZE |
					  UCP_PARAM_FIELD_REQUEST_INIT,
		.features = UCP_FEATURE_AMO64 | UCP_FEATURE_RMA,
		.request_size = sizeof(bk_req_t),
		.request_init = bk_req_init,
	};
	stat = ucp_init(&ucp_params, NULL, &bk_cfg->ucp_context);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	// create UCP worker
	ucp_worker_params_t worker_params = {
		.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE,
		.thread_mode = UCS_THREAD_MODE_MULTI,
	};

	stat = ucp_worker_create(bk_cfg->ucp_context, &worker_params, &bk_cfg->ucp_worker);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	stat = ucp_worker_get_address(bk_cfg->ucp_worker, &bk_cfg->ucp_address, &bk_cfg->ucp_address_len);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	// distribute rank 0 address and establish endpoint
	ucp_address_t* remote_addr = (0 == bk_cfg->mpi_rank) ? bk_cfg->ucp_address : malloc(bk_cfg->ucp_address_len);
	ret = MPI_Bcast(remote_addr, bk_cfg->ucp_address_len, MPI_BYTE, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);

	ucp_ep_params_t ep_params = {
		.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
		.address = remote_addr
	};
	stat = ucp_ep_create(bk_cfg->ucp_worker, &ep_params, &bk_cfg->ucp_ep);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	if (0 == bk_cfg->mpi_rank) {
		bk_syncroot_setup_ucp_mem(bk_cfg);
	}

	ret = MPI_Bcast(&bk_cfg->loc_ucp_ss.packed_counter_rkey_size, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);
	size_t packed_buf_size = bk_cfg->loc_ucp_ss.packed_counter_rkey_size;
	void* tmp_packed_rkey_buf = malloc(packed_buf_size);

	if (0 == bk_cfg->mpi_rank)memcpy(tmp_packed_rkey_buf, bk_cfg->loc_ucp_ss.packed_counter_rkey, packed_buf_size);
	ret = MPI_Bcast(tmp_packed_rkey_buf, packed_buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);
	stat = ucp_ep_rkey_unpack(bk_cfg->ucp_ep, tmp_packed_rkey_buf, &bk_cfg->remote_ucp_ss.counter_rkey);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	if (0 == bk_cfg->mpi_rank)memcpy(tmp_packed_rkey_buf, bk_cfg->loc_ucp_ss.packed_arr_rkey, packed_buf_size);
	ret = MPI_Bcast(tmp_packed_rkey_buf, packed_buf_size, MPI_BYTE, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);
	stat = ucp_ep_rkey_unpack(bk_cfg->ucp_ep, tmp_packed_rkey_buf, &bk_cfg->remote_ucp_ss.arr_rkey);
	BK_UCS_CHK(stat, bk_abort_wireup_ucp);

	if (0 == bk_cfg->mpi_rank)bk_cfg->remote_ucp_ss.counter_addr = bk_cfg->loc_ucp_ss.counter_addr;
	ret = MPI_Bcast(&bk_cfg->remote_ucp_ss.counter_addr, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);

	if (0 == bk_cfg->mpi_rank)bk_cfg->remote_ucp_ss.arr_addr = bk_cfg->loc_ucp_ss.arr_addr;
	ret = MPI_Bcast(&bk_cfg->remote_ucp_ss.arr_addr, 1, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_abort_wireup_ucp);

	// exchange UCP endpoints
bk_abort_wireup_ucp:
	if (0 == bk_cfg->mpi_rank)free(remote_addr);
	if (UCS_OK != stat)ret = MPI_ERR_UNKNOWN;
	return ret;
}

int bk_ucp_teardown(bk_synctest_config_t* bk_cfg) {
	ucs_status_ptr_t stat_ptr = UCS_OK;
	if (0 == bk_cfg->mpi_rank) {
		bk_ucp_syncroot_destroy_mem(bk_cfg);
	}

	if (NULL != bk_cfg->remote_ucp_ss.arr_rkey)
		ucp_rkey_destroy(bk_cfg->remote_ucp_ss.arr_rkey);
	if (NULL != bk_cfg->remote_ucp_ss.counter_rkey)
		ucp_rkey_destroy(bk_cfg->remote_ucp_ss.counter_rkey);

	if (NULL != bk_cfg->ucp_ep)
		stat_ptr = ucp_ep_close_nb(bk_cfg->ucp_ep, UCP_EP_CLOSE_MODE_FORCE);
	while (UCS_INPROGRESS == UCS_PTR_STATUS(stat_ptr))ucp_worker_progress(bk_cfg->ucp_worker);
	MPI_Barrier(MPI_COMM_WORLD);
	// ucp_worker_release_address(bk_config.ucp_worker, bk_config.ucp_address);
	ucp_worker_destroy(bk_cfg->ucp_worker);
	ucp_cleanup(bk_cfg->ucp_context);

	return MPI_SUCCESS;
}

int bk_finalize(bk_synctest_config_t* bk_cfg) {

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

inline ucs_status_t _bk_ucp_poll_completion(ucs_status_ptr_t status_ptr, bk_synctest_config_t* bk_cfg) {
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
		ucp_worker_progress(bk_cfg->ucp_worker);
	}

	status = req->status;
	req->completed = 0;
	req->status = UCS_INPROGRESS;
	ucp_request_free(status_ptr);
	return status;
}

int bk_ucp_reset_local(bk_synctest_config_t* bk_cfg) {
	if (0 != bk_cfg->mpi_rank) {
		BK_ERROR("Only rank 0 can call reset_local");
		return MPI_ERR_RANK;
	}
	bk_ucp_local_ss_t* loc_ss = &bk_cfg->loc_ucp_ss;
	int64_t* cnt = (int64_t*)loc_ss->counter_addr, * arr = (int64_t*)loc_ss->arr_addr;
	*cnt = -1;
	for (int i = 0; i < bk_cfg->mpi_size; i++) {
		arr[i] = -1;
	}
	return MPI_SUCCESS;
}

int check_str_num(const char* str, int ll, int ul, int* val) {
	*val = atoi(str);
	return (*val < ll || *val > ul) ? MPI_ERR_ARG : MPI_SUCCESS;
}


int process_cmd_flags(int argc, char* argv[], bk_exp_flags_t* flags) {
	extern char* optarg;
	extern int optind, optopt;
	char const* optstring = NULL;
	int c, ret = MPI_SUCCESS;
	int option_index = 0;

	static struct option long_options[] = {
			{"help",            no_argument,        0,  'h'},
			{"iterations",      required_argument,  0,  'i'},
			{"warmup",          required_argument,  0,  'x'},
			{"max-delay",       required_argument,  0,  'd'},
			{"verbose",         required_argument,  0,  'v'},
			{"experiment",      required_argument,  0,  'e'},
	};
	optstring = "+hi:x:d:v:e:";

	while (-1 != (c = getopt_long(argc, argv, optstring, long_options, &option_index))) {
		switch (c) {
		case'h':
			flags->print_help = 1;
			return MPI_SUCCESS;
		case'i':
			ret = check_str_num(optarg, 1, 100000, &flags->num_itters);
			break;
		case'x':
			ret = check_str_num(optarg, 0, 10000, &flags->num_warmups);
			break;
		case'd':
			ret = check_str_num(optarg, 0, 500, &flags->max_delay);
			break;
		case 'v':
			ret = check_str_num(optarg, 0, 100, &flags->verbosity);
			break;
		case 'e':
			ret = check_str_num(optarg, 0, BK_EXP_COUNT - 1, &flags->experiment);
			break;
		default:
			ret = MPI_ERR_ARG;
			break;
		}
		if (MPI_SUCCESS != ret) {
			BK_ERROR("Bad Arg -%c %s", c, optarg);
			return ret;
		}
	}
	return ret;
}

void bk_print_help_message() {
	char* out_str =
		"\nBK_SYNCSTRUCTURE_BENCHMARK\n"
		"--------------------------\n"
		"-h/--help				Print help message.\n"
		"-i/--iterations <>		Numer of benchmark iterations\n"
		"-x/--warmup <>			Number of warmup iterations\n"
		"-d/--max-delay <>		Max delay to apply, similar to MIF, but in us\n"
		"-v/--verbosity <>		Output verbosity\n"
		"-e/--experiment <>		Experiment to run {0:UCP_Atomic, 1:MPI_Atomic}\n"
		;
	fprintf(stdout, "%s", out_str);
}

int bk_mpi_wireup(bk_synctest_config_t* cfg) {
	int ret = MPI_SUCCESS;

	/* MPI_Aint win_size = (0 == cfg->mpi_rank) ? sizeof(int64_t) : 0; */
	MPI_Aint win_size = sizeof(int64_t);
	MPI_Aint win_disp = sizeof(int64_t);

	ret = MPI_Win_allocate(win_size, win_disp, MPI_INFO_NULL, MPI_COMM_WORLD, &cfg->mpi_cnt_baseptr, &cfg->mpi_cnt_win);
	BK_MPI_CHK(ret, bk_mpi_wireup_abort);

	/* win_size = (0 == cfg->mpi_rank) ? sizeof(int64_t) * cfg->mpi_size : 0; */
	win_size = sizeof(int64_t) * cfg->mpi_size; 
	win_disp = sizeof(int64_t);
	ret = MPI_Win_allocate(win_size, win_disp, MPI_INFO_NULL, MPI_COMM_WORLD, &cfg->mpi_arr_baseptr, &cfg->mpi_arr_win);
	BK_MPI_CHK(ret, bk_mpi_wireup_abort);

	if (0 == cfg->mpi_rank)
		bk_mpi_reset_local(cfg);


bk_mpi_wireup_abort:
	return ret;
}

int bk_mpi_teardown(bk_synctest_config_t* bk_cfg) {
	int ret = MPI_SUCCESS;

	ret = MPI_Win_free(&bk_cfg->mpi_cnt_win);
	BK_MPI_CHK(ret, bk_mpi_teardown_abort);

	ret = MPI_Win_free(&bk_cfg->mpi_arr_win);
	BK_MPI_CHK(ret, bk_mpi_teardown_abort);

bk_mpi_teardown_abort:
	return ret;
}

int bk_mpi_reset_local(bk_synctest_config_t* bk_cfg) {
	int ret = MPI_SUCCESS;
	int64_t* tmp = bk_cfg->mpi_cnt_baseptr, * tmp2 = bk_cfg->mpi_arr_baseptr;
	
	if (0 < bk_cfg->exp_flags.verbosity) {
		char* outstr = malloc(sizeof(*outstr) * bk_cfg->mpi_size * 5);
		outstr[0] = '\0';
		for (int i = 0; i < bk_cfg->mpi_size; i++){
			char tmpoutstr[16];
			sprintf(tmpoutstr, "%ld, ", tmp2[i]);
			strcat(outstr, tmpoutstr);
		}

		// BK_OUT("%ld, [%ld %ld %ld %ld]", *tmp, tmp2[0], tmp2[1], tmp2[2], tmp2[3]);
		BK_OUT("validate: %ld, [ %s]", *tmp, outstr);
		free(outstr);
	}


	*tmp = -1;
	for (int i = 0; i < bk_cfg->mpi_size; i++)
		tmp2[i] = -1;

	BK_MPI_CHK(ret, bk_mpi_reset_local_abort);
bk_mpi_reset_local_abort:
	return ret;
}
