#include <linux/membarrier.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <base/assert.h>
#include <base/compiler.h>

void membarrier_register(void)
{
	int ret;

	ret = syscall(__NR_membarrier,
		      MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED, 0, 0);
	BUG_ON(ret);
}

void membarrier(void)
{
	int ret;

	ret = syscall(__NR_membarrier,
		      MEMBARRIER_CMD_PRIVATE_EXPEDITED, 0, 0);
	BUG_ON(ret);
}
