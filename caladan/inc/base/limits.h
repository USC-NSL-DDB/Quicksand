/*
 * limits.h - maximum limits for different resources
 */

#pragma once

#ifndef NCPU
#define NCPU		256	/* max number of cpus */
#endif
#define NTHREAD		512	/* max number of threads */
#define NNUMA		4	/* max number of numa zones */
#define NSTAT		1024	/* max number of stat counters */
