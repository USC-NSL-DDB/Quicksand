#include <iokernel/control.h>

typedef void (*resource_pressure_handler)(void *args);

/* real-time resource pressure signals (shared with the iokernel) */
extern struct resource_pressure_info *resource_pressure_info;

extern void add_resource_pressure_handler(resource_pressure_handler handler,
                                          void *args);
