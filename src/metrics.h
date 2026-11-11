#ifndef METRICS_H
#define METRICS_H

#include <stddef.h>

void metrics_connection_opened(void);
void metrics_connection_closed(void);
void metrics_message_received(void);
void metrics_message_sent(void);
void metrics_message_dropped(void);
void metrics_bytes_in(size_t n);
void metrics_bytes_out(size_t n);

// 打印一行运行统计并 flush stdout
void metrics_dump(void);

#endif
