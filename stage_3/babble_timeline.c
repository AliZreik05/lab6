#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "babble_timeline.h"
#include "babble_server.h"
#include "babble_communication.h"

timeline_t* timeline_create(unsigned long client_key)
{
    timeline_t* tm= malloc(sizeof(timeline_t));
    if (tm == NULL) 
    {
    return NULL;
    }

    if (pthread_mutex_init(&tm->lock, NULL) != 0) 
    {
        free(tm);
        return NULL;
    }
    tm->youngest = 0;
    tm->count_recent_adds = 0;
    tm->key = client_key;
    
    return tm;
}

void timeline_free(timeline_t *timeline)
{
    pthread_mutex_destroy(&timeline->lock);
    free(timeline);
}


time_t timeline_insert(timeline_t *tm, client_bundle_t *publisher, char *msg)
{
    struct timespec tt;
    
    clock_gettime(CLOCK_REALTIME, &tt);

    pthread_mutex_lock(&tm->lock);
    publication_t *pub = &tm->circular_buffer[tm->youngest];
    memset(pub->msg, 0, sizeof pub->msg);

    pub->date = tt.tv_sec - server_start;
    snprintf(pub->msg, BABBLE_BUFFER_SIZE,"    %s[%ld]: %s\n", publisher->client_name, pub->date, msg);
    
    /* shifting the index */
    tm->youngest = (tm->youngest + 1) % BABBLE_TIMELINE_MSIZE;

    tm->count_recent_adds++;

    time_t date = pub->date;
    pthread_mutex_unlock(&tm->lock);
    return date;
}

void timeline_generate_summary(timeline_t *tm, answer_t **answer)
{
    answer_t *the_answer=NULL;
    unsigned int index_first=0;

    the_answer = alloc_answer(tm->key);
    
    /* the first msg of the answer is the number of publications since
     * the last call to timeline */   
    pthread_mutex_lock(&tm->lock); 
    add_msg_to_answer(the_answer, sizeof(unsigned int), &tm->count_recent_adds);

    /* compute the index of the first msg to add to the timeline */
    if(tm->count_recent_adds >= BABBLE_TIMELINE_MSIZE){
        index_first = tm->youngest;
        
        /* deal with the corner case where the buffer is full */
        add_msg_to_answer(the_answer, BABBLE_BUFFER_SIZE, &tm->circular_buffer[index_first]);
        index_first = (index_first + 1) % BABBLE_TIMELINE_MSIZE;
    }
    else{
        index_first = (BABBLE_TIMELINE_MSIZE + tm->youngest - tm->count_recent_adds) % BABBLE_TIMELINE_MSIZE;
    }
    
    /* add all new msgs in the timeline */
    while(index_first != tm->youngest ){
        add_msg_to_answer(the_answer, BABBLE_BUFFER_SIZE, &tm->circular_buffer[index_first]);

        index_first = (index_first + 1) % BABBLE_TIMELINE_MSIZE;
    }

    tm->count_recent_adds = 0;
    pthread_mutex_unlock(&tm->lock);
    
    *answer = the_answer;
}
