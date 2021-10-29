#include <iokernel/control.h>

/* real-time resource pressure signals (shared with the iokernel) */
extern struct resource_pressure_info *resource_pressure_info;

typedef void (*resource_pressure_handler)(void);

extern void register_resource_pressure_handler(resource_pressure_handler h);
extern void deregister_resource_pressure_handler(void);
