#ifndef __BABBLE_TYPES_H__
#define __BABBLE_TYPES_H__

#include <time.h>
#include <pthread.h>

#include "babble_config.h"
#include <stdatomic.h>

/* forward declaration, defined in babble_timeline.h */
struct timeline;

typedef enum{
    LOGIN =0,
    PUBLISH,
    FOLLOW,
    TIMELINE,
    FOLLOW_COUNT,
    RDV,
    UNREGISTER,
    PARSE_ERROR
} command_id;

typedef struct command{
    command_id cid;
    int sockfd;    /* only needed by the LOGIN command, other commands
                  * will use the key */
    unsigned long key;
    char msg[BABBLE_PUBLICATION_MSIZE + 1];
    int answer_expected;   /* answer sent only if set */
} command_t;

typedef struct client_bundle{
    unsigned long key;     /* hash of the name */
    char client_name[BABBLE_ID_MSIZE];    /* name as provided by the
                                          * client */
    int sockfd;              /* socket to communicate with this client */
    struct timeline *timeline;   /* timeline of the client */
    pthread_mutex_t followers_lock;
    struct client_bundle *followers[MAX_CLIENT];
    unsigned int nb_followers;
    atomic_uint disconnected;

} client_bundle_t;


#endif
