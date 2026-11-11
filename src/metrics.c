#include "metrics.h"
#include "connection.h"
#include "retained.h"
#include "session.h"
#include "subscription.h"

#include <stdio.h>

static unsigned long long g_conn_accepted;
static unsigned long long g_conn_closed;
static unsigned long long g_messages_received;
static unsigned long long g_messages_sent;
static unsigned long long g_messages_dropped;
static unsigned long long g_bytes_in;
static unsigned long long g_bytes_out;

void metrics_connection_opened(void) { g_conn_accepted++; }
void metrics_connection_closed(void) { g_conn_closed++; }
void metrics_message_received(void) { g_messages_received++; }
void metrics_message_sent(void) { g_messages_sent++; }
void metrics_message_dropped(void) { g_messages_dropped++; }
void metrics_bytes_in(size_t n) { g_bytes_in += n; }
void metrics_bytes_out(size_t n) { g_bytes_out += n; }

void metrics_dump(void) {
    int conn_active = 0;
    for (connection_t *c = connection_first(); c; c = connection_next(c)) {
        if (!c->dead)
            conn_active++;
    }

    int inflight = 0;
    int queued = 0;
    for (session_t *s = session_first(); s; s = session_next(s)) {
        inflight += s->inflight_count;
        queued += s->queue_count;
    }

    printf("stats: conn_active=%d conn_accepted=%llu conn_closed=%llu "
           "sessions_persistent=%d subscriptions=%d retained=%d inflight=%d "
           "queued=%d messages_received=%llu messages_sent=%llu "
           "messages_dropped=%llu bytes_in=%llu bytes_out=%llu\n",
           conn_active, g_conn_accepted, g_conn_closed,
           session_persistent_count(), subscription_count(), retained_count(),
           inflight, queued, g_messages_received, g_messages_sent,
           g_messages_dropped, g_bytes_in, g_bytes_out);
    fflush(stdout);
}
