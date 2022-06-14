#include <bk_ss_lib.h>
#include <stdio.h>
#include <stdlib.h>

// #define BK_ITTERS 1000
#define BK_ITTERS 100

int mca_coll_bkpap_arrive_ss();

int main(int argc, char** argv) {
	int ret = MPI_SUCCESS;

	ret = bk_init(&argc, &argv);
	BK_MPI_CHK(ret, bk_main_abort);
	ret = bk_wireup_ucp();
	BK_MPI_CHK(ret, bk_main_abort);

	for (int i = 0; i < BK_ITTERS; i++) {
		ret = MPI_Barrier(MPI_COMM_WORLD);
		BK_MPI_CHK(ret, bk_main_abort);

		int64_t reply_buffer, val_buf = 1;
		
		ret = mca_coll_bkpap_arrive_ss(&reply_buffer);
		BK_MPI_CHK(ret, bk_main_abort);

		MPI_Request ibarrier_req;
		int ibarrier_flag;
		ret = MPI_Ibarrier(MPI_COMM_WORLD, &ibarrier_req);
		BK_MPI_CHK(ret, bk_main_abort);
		MPI_Test(&ibarrier_req, &ibarrier_flag, MPI_STATUS_IGNORE);

		while (!ibarrier_flag) {
			ucp_worker_progress(bk_config.ucp_worker);
			MPI_Test(&ibarrier_req, &ibarrier_flag, MPI_STATUS_IGNORE);
		}
		ret = MPI_Barrier(MPI_COMM_WORLD);
		BK_MPI_CHK(ret, bk_main_abort);

		BK_OUT("reply_buffer: %ld", reply_buffer);
		if (0 == bk_config.mpi_rank) {
			BK_OUT("counter: %ld", *(int64_t*)bk_config.loc_ss.counter_addr);
		}
		if(0 == bk_config.mpi_rank)bk_reset_local();
		fflush(stdout);
	}

	ret = bk_finalize();
	BK_MPI_CHK(ret, bk_main_abort);

bk_main_abort:
	return ret;
}

int mca_coll_bkpap_arrive_ss(int64_t* ret_pos) {
	bk_remote_ss_t* remote_ss = &bk_config.remote_ss;
	ucs_status_ptr_t status_ptr = NULL;
	ucs_status_t status = UCS_OK;
	int64_t reply_buf = -1, op_buf = 1;
	uint64_t put_buf = bk_config.mpi_rank;

	uint64_t counter_addr = remote_ss->counter_addr;
	uint64_t arrival_arr_addr = remote_ss->arr_addr;

	ucp_request_param_t req_params = {
		.op_attr_mask = UCP_OP_ATTR_FIELD_REPLY_BUFFER |
			UCP_OP_ATTR_FIELD_CALLBACK |
			UCP_OP_ATTR_FIELD_DATATYPE,
		.cb.send = _bk_send_cb,
		.user_data = NULL,
		.datatype = ucp_dt_make_contig(sizeof(reply_buf)),
		.reply_buffer = &reply_buf,
	};

	// TODO: Could try to be hardcore and move the arrival_arr put into the counter_fadd callback 
	status_ptr = ucp_atomic_op_nbx(
		bk_config.ucp_ep, UCP_ATOMIC_OP_ADD, &op_buf, 1,
		counter_addr, remote_ss->counter_rkey, &req_params);
	if (UCS_PTR_IS_ERR(status_ptr)) {
		status = UCS_PTR_STATUS(status_ptr);
		BK_ERROR("atomic_op_nbx failed code %d (%s)", status, ucs_status_string(status));
		ucp_request_free(status_ptr);
		return MPI_ERR_UNKNOWN;
	}
	status = _bk_poll_completion(status_ptr);
	if (UCS_OK != status) {
		BK_ERROR("_bk_poll_completion failed code: %d, (%s)", status, ucs_status_string(status));
		return MPI_ERR_UNKNOWN;
	}

	uint64_t put_addr = arrival_arr_addr + ((reply_buf + 1) * sizeof(int64_t));
	req_params.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK;
	status_ptr = ucp_put_nbx(
		bk_config.ucp_ep, &put_buf, sizeof(put_buf),
		put_addr, remote_ss->arr_rkey, &req_params);

	status = _bk_poll_completion(status_ptr);
	if (UCS_OK != status) {
		BK_ERROR("_bk_poll_completion failed code: %d, (%s)", status, ucs_status_string(status));
		return MPI_ERR_UNKNOWN;
	}

	*ret_pos = reply_buf;
	return MPI_SUCCESS;
}