#include <stdint.h>
#include <sys/sysinfo.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"

void ias_ps_poll(uint64_t now_us)
{
	struct sysinfo info;
	int64_t free_ram_mbs, used_swap_mbs, to_release_mem_mbs;
	struct congestion_info *congestion;
	struct ias_data *sd;

	BUG_ON(sysinfo(&info) != 0);
	used_swap_mbs = (info.totalswap - info.freeswap) / SIZE_MB;
	free_ram_mbs = info.freeram / SIZE_MB - used_swap_mbs;
	if (free_ram_mbs < IAS_PS_MEM_LOW_MB)
		to_release_mem_mbs = IAS_PS_MEM_LOW_MB - free_ram_mbs;
	else
		to_release_mem_mbs = 0;

	ias_for_each_proc(sd) {
		congestion = sd->p->congestion_info;
		congestion->free_mem_mbs = free_ram_mbs;
		congestion->idle_num_cores = ias_num_idle_cores;

		/* Memory pressure. */
		if (sd->react_mem_pressure)
			congestion->to_release_mem_mbs = to_release_mem_mbs;

		/* CPU pressure. */
		if (sd->react_cpu_pressure) {
			if (sd->is_congested) {
				if (!sd->cpu_pressure_start_us)
					sd->cpu_pressure_start_us = now_us;
				else if (now_us - sd->cpu_pressure_start_us >=
					 IAS_PS_CPU_THRESH_US)
					congestion->cpu_pressure = true;
			} else {
				sd->cpu_pressure_start_us = 0;
				congestion->cpu_pressure = false;
			}
		}
        }
}
