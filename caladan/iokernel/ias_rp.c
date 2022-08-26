#include <stdint.h>

#include <base/hash.h>
#include <base/log.h>
#include <base/stddef.h>

#include "defs.h"
#include "ias.h"
#include "ksched.h"

#define MAX_INTERVAL_US 2000

static bool ias_rp_preempt_core(struct ias_data *sd)
{
	unsigned int i, core;
	struct thread *th;

	for (i = 0; i < sd->p->active_thread_count; i++)
		if (!(*sd->p->active_threads[i]->preemptor))
			break;

	if (unlikely(i == sd->p->active_thread_count)) {
		if (likely(ias_add_kthread(sd) == 0))
			i = 0;
		else
			return false;
	}

	core = sd->p->active_threads[i]->core;
	/* Grant exclusive access by marking the core as reserved. */
	if (!bitmap_test(sd->reserved_cores, core)) {
		bitmap_set(sd->reserved_cores, core);
		bitmap_set(sd->reserved_report_handler_cores, core);
	}

	th = sd->p->active_threads[i];
	*th->preemptor = sd->p->resource_reporting->handler;
	barrier();
	ksched_run(core, th->tid);
	ksched_enqueue_intr(core, KSCHED_INTR_YIELD);

	return true;
}

void ias_rp_poll(void)
{
	struct ias_data *sd;
	int pos;
	struct resource_reporting *report;
	uint64_t now_tsc = rdtsc();

	ias_for_each_proc(sd) {
		report = sd->p->resource_reporting;
		if (report->status == HANDLED) {
		        /* Take away the exclusive access. */
	                bitmap_for_each_set(sd->reserved_report_handler_cores,
					    NCPU, pos) {
				bitmap_clear(sd->reserved_report_handler_cores,
					     pos);
				bitmap_clear(sd->reserved_cores, pos);
	                }
			report->status = NONE;
		}

		if (report->status == NONE) {
                        if (report->last_tsc + MAX_INTERVAL_US * cycles_per_us <
			    now_tsc) {
			        if (report->handler &&
			            likely(ias_rp_preempt_core(sd)))
			                report->status = HANDLING;
			}
		}
        }
}
