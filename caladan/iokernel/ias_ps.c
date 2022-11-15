#include <stdint.h>
#include <sys/sysinfo.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"
#include "sched.h"

#define MIN_PREEMPT_INTERVAL_US 100

static bool ias_ps_preempt_core(struct ias_data *sd)
{
	unsigned int num_needed_cores, num_eligible_cores = 0, i, core;
	void **preemptor_ptr;
	struct thread *th;

	for (i = 0; i < sd->p->active_thread_count; i++)
		num_eligible_cores += !(*sd->p->active_threads[i]->preemptor);

	num_needed_cores = *sd->p->num_resource_pressure_handlers;
	while (num_eligible_cores < num_needed_cores) {
		if (unlikely(ias_add_kthread(sd) != 0))
			return false;
		th = sd->p->active_threads[sd->p->active_thread_count - 1];
		num_eligible_cores += !(*th->preemptor);
	}

	/* Preempt the first num_needed_cores cores. */
	i = 0;
	while (num_needed_cores) {
		core = sd->p->active_threads[i]->core;
		th = sd->p->active_threads[i++];
		preemptor_ptr = th->preemptor;

		if (unlikely(*preemptor_ptr))
                        continue;

		/* Grant exclusive access by marking the core as reserved. */
		if (!bitmap_test(sd->reserved_cores, core)) {
			bitmap_set(sd->reserved_cores, core);
			bitmap_set(sd->reserved_pressure_handler_cores, core);
		}
		*preemptor_ptr = sd->p->resource_pressure_handlers[--num_needed_cores];
		barrier();
		/* Handle the race condition of thread parking. */
		ksched_run(core, th->tid);
		BUG_ON(sched_yield_on_core(core) != 0);
	}

	return true;
}

void ias_ps_poll(uint64_t now_us)
{
	bool has_pressure;
	struct sysinfo info;
	int64_t free_ram_mbs, used_swap_mbs, to_release_mem_mbs;
	struct congestion_info *congestion;
	struct resource_pressure_info *pressure;
	struct ias_data *sd;
	int pos;

	BUG_ON(sysinfo(&info) != 0);
	used_swap_mbs = (info.totalswap - info.freeswap) / SIZE_MB;
	free_ram_mbs = info.freeram / SIZE_MB - used_swap_mbs;
	if (free_ram_mbs < IAS_PS_MEM_LOW_MB)
		to_release_mem_mbs = IAS_PS_MEM_LOW_MB - free_ram_mbs;
	else
		to_release_mem_mbs = 0;

	ias_for_each_proc(sd) {
		pressure = sd->p->resource_pressure_info;
		congestion = sd->p->congestion_info;
		congestion->free_mem_mbs = free_ram_mbs;
		congestion->idle_num_cores = ias_num_idle_cores;
		has_pressure = false;

		if (pressure->mock) {
			has_pressure = true;
			goto done_pressure_update;
		}

		/* Memory pressure. */
		if (sd->react_mem_pressure) {
			pressure->to_release_mem_mbs =
				to_release_mem_mbs;
			has_pressure = to_release_mem_mbs;
		}

		/* CPU pressure. */
		if (sd->react_cpu_pressure) {
			if (sd->is_congested) {
				if (!sd->cpu_pressure_start_us)
					sd->cpu_pressure_start_us = now_us;
				else if (now_us - sd->cpu_pressure_start_us >=
					 IAS_PS_CPU_THRESH_US) {
					pressure->cpu_pressure = true;
					has_pressure = true;
				}
			} else {
				sd->cpu_pressure_start_us = 0;
				pressure->cpu_pressure = false;
			}
		}

done_pressure_update:
		if (pressure->status == HANDLED) {
		        /* Take away the exclusive access. */
	                bitmap_for_each_set(sd->reserved_pressure_handler_cores,
					    NCPU, pos) {
				bitmap_clear(sd->reserved_pressure_handler_cores,
					     pos);
				bitmap_clear(sd->reserved_cores, pos);
	                }
			pressure->status = NONE;
		}

		if (pressure->status == NONE && has_pressure) {
                        if (now_us - pressure->last_preempt_us >=
			    MIN_PREEMPT_INTERVAL_US) {
                                pressure->last_preempt_us = now_us;
                                if (likely(ias_ps_preempt_core(sd)))
                                        pressure->status = HANDLING;
                        }
		}
        }
}
