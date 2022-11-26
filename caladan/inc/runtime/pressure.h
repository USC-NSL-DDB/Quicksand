#include <iokernel/control.h>

typedef void (*resource_pressure_handler)(void *args);

extern void add_resource_pressure_handler(resource_pressure_handler handler,
                                          void *args);
extern void remove_all_resource_pressure_handlers(void);
