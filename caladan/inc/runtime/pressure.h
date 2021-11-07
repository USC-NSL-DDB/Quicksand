#include <iokernel/control.h>

/* real-time resource pressure signals (shared with the iokernel) */
extern struct resource_pressure_info *resource_pressure_info;

typedef void (*resource_pressure_handler)(void *args);

extern void add_resource_pressure_handler(resource_pressure_handler handler,
                                          void *args);
