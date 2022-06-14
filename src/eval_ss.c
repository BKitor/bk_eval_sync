#include <bk_ss_lib.h>
#include <stdio.h>
#include <stdlib.h>


int mca_coll_bkpap_arrive_ss();

int main(int argc, char** argv) {
	bk_exp_flags_t* flags;
	int ret = MPI_SUCCESS;
	double exp_time = 0;

	ret = bk_init(argc, argv);
	BK_MPI_CHK(ret, bk_main_abort);
	flags = &bk_config.exp_flags;

	if (1 == bk_config.exp_flags.print_help) {
		if (0 == bk_config.mpi_rank)bk_print_help_message();
		return 0;
	}

	ret = bk_wireup_ucp();
	BK_MPI_CHK(ret, bk_main_abort);

	for (int bmark_itter = 0; bmark_itter < flags->num_itters + flags->num_warmups; bmark_itter++) {
		int64_t reply_buffer;
		double s_time, e_time;
		int exp_num = bmark_itter - flags->num_warmups;

		ret = MPI_Barrier(MPI_COMM_WORLD);
		BK_MPI_CHK(ret, bk_main_abort);

		// Measure execution 
		s_time = MPI_Wtime();
		ret = mca_coll_bkpap_arrive_ss(&reply_buffer);
		e_time = MPI_Wtime();
		BK_MPI_CHK(ret, bk_main_abort);

		// Complete ensure all procs exit, 
		// necesary for SHM transport, most likely useless for IB
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

		// if not on a warmup run
		if (exp_num >= 0) {
			double run_time = (e_time - s_time) * 1e6;
			exp_time += run_time / ((double)flags->num_itters);
			if (flags->verbosity > 0) {
				BK_OUT("iter: %d, time: %fus", exp_num, run_time);
			}
		}

		if (0 == bk_config.mpi_rank)bk_reset_local();
		fflush(stdout);
	}

	double global_exp_time;
	ret = MPI_Reduce(&exp_time, &global_exp_time, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
	BK_MPI_CHK(ret, bk_main_abort);
	global_exp_time /= bk_config.mpi_size;

	if (0 == bk_config.mpi_rank)
		BK_OUT("num ranks: %d, num_itters: %d,  num_warmups: %d, avg_time %fus", bk_config.mpi_size, flags->num_itters, flags->num_warmups, global_exp_time);

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