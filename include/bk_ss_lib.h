#include <ucp/api/ucp.h>
#include "mpi.h"
#include <stdlib.h>
#include <string.h>

#define BK_OUT(_str,...) printf("BK_OUT rank: %d: "_str"\n", bk_config.mpi_rank, ##__VA_ARGS__);
#define BK_ERROR(_str,...) fprintf(stderr, "BK_ERROR (rank: %d) %s:%d : "_str"\n",bk_config.mpi_rank, __FILE__, __LINE__, ##__VA_ARGS__);

#define BK_MPI_CHK(_ret, _lbl) do { \
	if(MPI_SUCCESS != _ret){ \
		int err_str_len; \
		char err_str[MPI_MAX_ERROR_STRING]; \
		MPI_Error_string(_ret, err_str, &err_str_len); \
		BK_ERROR("MPI error code: %d (%s)", _ret, err_str); \
		goto _lbl;\
	} \
}while(0);

#define BK_UCS_CHK(_ret, _lbl) do { \
	if(UCS_OK != _ret){ \
		BK_ERROR("UCS error code: %d, (%s)", _ret, ucs_status_string(_ret)); \
		goto _lbl;\
	} \
}while(0);

typedef struct bk_local_ss {
	ucp_mem_h counter_mem_h;
	size_t counter_buf_size;
	uint64_t counter_addr;
	void* packed_counter_rkey;
	size_t packed_counter_rkey_size;
	ucp_mem_h arr_mem_h;
	size_t arr_buf_size;
	uint64_t arr_addr;
	void* packed_arr_rkey;
	size_t packed_arr_rkey_size;
} bk_local_ss_t;

typedef struct bk_remote_ss {
	uint64_t counter_addr;
	ucp_rkey_h counter_rkey;
	uint64_t arr_addr;
	ucp_rkey_h arr_rkey;
} bk_remote_ss_t;

typedef struct bk_synctest_config {
	ucp_context_h ucp_context;
	ucp_worker_h ucp_worker;
	ucp_address_t* ucp_address;
	size_t ucp_address_len;
	ucp_ep_h ucp_ep;

	bk_local_ss_t loc_ss;
	bk_remote_ss_t remote_ss;

	int mpi_rank;
	int mpi_size;
} bk_synctest_config_t;

bk_synctest_config_t bk_config;

typedef struct bk_req {
	ucs_status_t status;
	int completed;
} bk_req_t;

void bk_req_init(void* req);

int bk_init(int* argc_ptr, char*** argv_ptr);
int bk_wireup_ucp();
int bk_finalize();
int bk_reset_local();
ucs_status_t _bk_poll_completion(ucs_status_ptr_t status_ptr);
void _bk_send_cb(void* request, ucs_status_t status, void* args);