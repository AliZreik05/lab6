#include "babble_communication.h"
#include "babble_types.h"

#include <unistd.h>
#include <limits.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <errno.h>

/* Transfer a complete frame field, retrying interrupted system calls. */
static int write_data(int fd, unsigned long size, const void *buf)
{
    unsigned long total_sent = 0;
    while (total_sent < size) {
        ssize_t sent = send(fd, (const char *)buf + total_sent,
                            size - total_sent, MSG_NOSIGNAL);
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent <= 0) {
            return -1;
        }
        total_sent += sent;
    }
    return 0;
}

static int read_data(int fd, unsigned long size, void *buf)
{
    unsigned long total_recv = 0;
    while (total_recv < size) {
        ssize_t received = read(fd, (char *)buf + total_recv, size - total_recv);
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received <= 0) {
            return -1;
        }
        total_recv += received;
    }
    return 0;
}

int network_send(int fd, unsigned long size, void *buf)
{
    if (size > INT_MAX ||
        write_data(fd, sizeof size, &size) < 0 ||
        write_data(fd, size, buf) < 0) {
        return -1;
    }
    return size;
}

int network_recv(int fd, void **buf)
{
    unsigned long payload_size;
    *buf = NULL;

    if (read_data(fd, sizeof payload_size, &payload_size) < 0 ||
        payload_size > INT_MAX) {
        return -1;
    }
    if (payload_size == 0) {
        return 0;
    }

    void *payload = malloc(payload_size);
    if (payload == NULL) {
        return -1;
    }
    if (read_data(fd, payload_size, payload) < 0) {
        free(payload);
        return -1;
    }

    *buf = payload;
    return payload_size;
}
