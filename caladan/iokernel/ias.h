/*
 * ias.h - the shared header for the IAS scheduler
 */

#pragma once

#include "ias_constant.h"

/*
 * Data structures
 */

struct ias_data {
	struct proc		*p;
	unsigned int		is_congested:1;
	unsigned int		is_bwlimited:1;
	unsigned int		is_lc:1;
	unsigned int		idx; /* a unique index */
	uint64_t		qdelay_us;
	uint64_t                quantum_us;
	struct list_node	all_link;
	DEFINE_BITMAP(reserved_cores, NCPU);
	DEFINE_BITMAP(reserved_ps_cores, NCPU);
	DEFINE_BITMAP(reserved_rp_cores, NCPU);

	/* thread usage limits */
	int			threads_guaranteed;/* the number promised */
	int			threads_max;	/* the most possible */
	int			threads_limit;	/* the most allowed */
	int			threads_active;	/* the number active */

	/* locality subcontroller */
	uint64_t		loc_last_us[NCPU];

	/* the hyperthread subcontroller */
	uint64_t		ht_punish_us;
	uint64_t		ht_punish_count;

	/* memory bandwidth subcontroller */
	float			bw_llc_miss_rate;

	/* will it react to resource pressure? */
	bool                    react_mem_pressure;
	bool                    react_cpu_pressure;
	/* used for monitoring the duration of cpu pressure */
	uint64_t                cpu_pressure_start_us;
};

extern struct list_head all_procs;
extern struct ias_data *cores[NCPU];
extern uint64_t ias_gen[NCPU];
extern uint64_t now_us;

/**
 * ias_for_each_proc - iterates through all processes
 * @proc: a pointer to the current process in the list
 */
#define ias_for_each_proc(proc) \
	list_for_each(&all_procs, proc, all_link)

extern int ias_idle_placeholder_on_core(struct ias_data *sd, unsigned int core);
extern int ias_idle_on_core(unsigned int core);
extern bool ias_can_add_kthread(struct ias_data *sd, bool ignore_ht_punish_cores);
extern int ias_add_kthread(struct ias_data *sd);
extern int ias_add_kthread_on_core(unsigned int core);
extern int ias_run_kthread_on_core(struct ias_data *sd, unsigned int core);


/*
 * Hyperthread (HT) subcontroller definitions
 */

DECLARE_BITMAP(ias_ht_punished_cores, NCPU);

struct ias_ht_data {
	/* the fraction of the punish budget used so far */
	float		budget_used;
};

extern struct ias_ht_data ias_ht_percore[NCPU];

extern void ias_ht_poll(void);
extern unsigned int ias_ht_relinquish_core(struct ias_data *sd);

static inline float ias_ht_budget_used(unsigned int core)
{
	return ias_ht_percore[core].budget_used;
}

/*
 * Bandwidth (BW) subcontroller definitions
 */

extern void ias_bw_poll(void);
extern int ias_bw_init(void);
extern float ias_bw_estimate_multiplier;

/*
 * Resource Pressure (PS) subcontroller definitions
 */

extern void ias_ps_poll(void);

/*
 * Resource Reporting (RP) subcontroller definitions
 */

extern void ias_rp_poll(void);

/*
 * Time sharing (TS) subcontroller definitions
 */

extern void ias_ts_poll(void);


/*
 * Counters
 */

extern uint32_t ias_num_idle_cores;
extern uint64_t ias_bw_punish_count;
extern uint64_t ias_bw_relax_count;
extern float    ias_bw_estimate;
extern uint64_t	ias_bw_sample_failures;
extern uint64_t ias_bw_sample_aborts;
extern uint64_t ias_ht_punish_count;
extern uint64_t ias_ht_relax_count;
extern uint64_t ias_ts_yield_count;
