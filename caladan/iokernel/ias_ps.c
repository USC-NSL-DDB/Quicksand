#include <stdint.h>
#include <sys/sysinfo.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"

static bool ias_ps_preempt_core(struct ias_data *sd)
{
	unsigned int num_cores_needed, i, core;

	num_cores_needed = *sd->p->num_resource_pressure_handlers;
	while (sd->p->active_thread_count < num_cores_needed)
		if (unlikely(ias_add_kthread(sd) != 0))
			return false;

	/* Preempt the first num_cores_needed cores. */
	for (i = 0; i < num_cores_needed; i++) {
		core = sd->p->active_threads[i]->core;
		/* Grant exclusive access by marking the core as reserved. */
		if (!bitmap_test(sd->reserved_cores, core)) {
			bitmap_set(sd->reserved_cores, core);
			bitmap_set(sd->reserved_handler_cores, core);
		}
		*sd->p->active_threads[i]->preemptor =
			sd->p->resource_pressure_handlers[i];
		ksched_enqueue_intr(core, KSCHED_INTR_YIELD);
	}

	return true;
}

bool ias_ps_poll(void)
{
	bool success = true, has_pressure;
	struct sysinfo info;
	uint64_t free_ram_in_mbs, mem_mbs_to_release;
	struct resource_pressure_info *pressure;
	struct ias_data *sd;
	int num_cores_taken, pos;

	BUG_ON(sysinfo(&info) != 0);
	free_ram_in_mbs = info.freeram / SIZE_MB;
	if (free_ram_in_mbs < IAS_PS_MEM_LOW_MB)
		mem_mbs_to_release = IAS_PS_MEM_LOW_MB - free_ram_in_mbs;
	else
		mem_mbs_to_release = 0;

	ias_for_each_proc(sd) {
		pressure = sd->p->resource_pressure_info;

		has_pressure = false;
		if (pressure->status == HANDLED) {
		        /* Take away the exclusive access. */
	                bitmap_for_each_set(sd->reserved_handler_cores, NCPU, pos) {
				bitmap_clear(sd->reserved_handler_cores, pos);
				bitmap_clear(sd->reserved_cores, pos);
	                }

			/* Memory pressure. */
	                if (sd->react_mem_pressure && mem_mbs_to_release) {
				pressure->mem_mbs_to_release =
					mem_mbs_to_release;
				mem_mbs_to_release = 0;
				has_pressure = true;
			}

			/* CPU pressure. */
	                if (sd->react_cpu_pressure && sd->is_congested) {
				num_cores_taken = pressure->num_cores_granted -
                                                  sd->threads_active;
				if (num_cores_taken > 0) {
					pressure->num_cores_to_release =
                                                num_cores_taken;
					pressure->num_cores_granted =
						sd->threads_active;
					has_pressure = true;
				}
	                }

			if (has_pressure)
				store_release(&pressure->status, PENDING);
		}

		if (pressure->status == PENDING) {
			if (likely(ias_ps_preempt_core(sd)))
				pressure->status = HANDLING;
			else
				success = false;
		}
        }

	return success;
}
