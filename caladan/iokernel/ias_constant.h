#pragma once

/*
 * Constant tunables
 */

/* the maximum number of processes */
#define IAS_NPROC			32
/* the memory bandwidth limit */
#define IAS_BW_LIMIT			25000.0
/* the interval that each subcontroller polls */
#define IAS_POLL_INTERVAL_US		10
/* the low watermark used to detect memory pressure */
#define IAS_PS_MEM_LOW_MB 		1024
/* the threshold of cpu pressure duration to trigger migration */
#define IAS_PS_CPU_THRESH_US            5000
/* the interval to trigger PS subcontroller */
#define IAS_PS_INTERVAL_US              500
/* the interval to trigger RP subcontroller */
#define IAS_RP_INTERVAL_US              250
/* the time before the core-local cache is assumed to be evicted */
#define IAS_LOC_EVICTED_US		100
/* the debug info printing interval */
#define IAS_DEBUG_PRINT_US		1000000

