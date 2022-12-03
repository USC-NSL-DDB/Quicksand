#include <stdint.h>
#include <sys/sysinfo.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"
#include "sched.h"

static void ias_ps_preempt_core(struct ias_data *sd)
{
	unsigned int num_needed_cores, num_eligible_cores = 0, i, core;
	struct thread *th;
	uint64_t now_tsc;

	for (i = 0; i < sd->p->active_thread_count; i++)
		num_eligible_cores += !sd->p->active_threads[i]->preemptor->th;

	num_needed_cores = *sd->p->num_resource_pressure_handlers;
	while (num_eligible_cores < num_needed_cores) {
		if (unlikely(ias_add_kthread(sd) != 0))
			return;
		th = sd->p->active_threads[sd->p->active_thread_count - 1];
		num_eligible_cores += !th->preemptor->th;
	}

	sd->p->resource_pressure_info->last_us = now_us;
	sd->p->resource_pressure_info->status = HANDLING;
	mb();
	if (unlikely(!ACCESS_ONCE(sd->p->num_resource_pressure_handlers))) {
		sd->p->resource_pressure_info->status = NONE;
		return;
	}

	/* Preempt the first num_needed_cores cores. */
	i = 0;
	now_tsc = rdtsc();
	while (num_needed_cores) {
		core = sd->p->active_threads[i]->core;
		th = sd->p->active_threads[i++];
		if (unlikely(th->preemptor->th))
                        continue;
		th->preemptor->th =
                        sd->p->resource_pressure_handlers[--num_needed_cores];
		th->preemptor->ready_tsc = now_tsc;
		barrier();
		sched_yield_on_core(core);
	}
}

void ias_ps_poll(void)
{
	bool has_pressure;
	struct sysinfo info;
	int64_t free_ram_mbs, used_swap_mbs, to_release_mem_mbs;
	struct congestion_info *congestion;
	struct resource_pressure_info *pressure;
	struct ias_data *sd;

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
			pressure->to_release_mem_mbs = INT_MAX;
			has_pressure = true;
			goto update_fsm;
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

	update_fsm:
		if (pressure->status == HANDLED)
			pressure->status = NONE;

		if (pressure->status == NONE && has_pressure)
			if (pressure->last_us + IAS_PS_INTERVAL_US < now_us)
                                ias_ps_preempt_core(sd);
        }
}
