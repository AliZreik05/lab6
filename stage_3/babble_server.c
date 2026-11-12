#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>

#include "babble_server.h"
#include "babble_types.h"
#include "babble_utils.h"
#include "babble_communication.h"
#include "babble_server_answer.h"
#include "fastrand.h"


/* to activate random delays in the processing of messages */
int random_delay_activated;

static command_t *command_buffer[BABBLE_PRODCONS_SIZE];
static command_t *active_commands[BABBLE_EXECUTOR_THREADS];
static int command_count = 0;

static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;
static pthread_cond_t work_available = PTHREAD_COND_INITIALIZER;

static void enqueue_command(command_t *cmd)
{
    pthread_mutex_lock(&queue_mutex);

    while (command_count == BABBLE_PRODCONS_SIZE) 
    {
        pthread_cond_wait(&not_full, &queue_mutex);
    }

    command_buffer[command_count++] = cmd;

    pthread_cond_signal(&work_available);
    pthread_mutex_unlock(&queue_mutex);
}

/* Called with queue_mutex held. */
static int client_has_active_command(unsigned long key)
{
    for (int i = 0; i < BABBLE_EXECUTOR_THREADS; i++) {
        if (active_commands[i] != NULL && active_commands[i]->key == key) {
            return 1;
        }
    }
    return 0;
}

/* Called with queue_mutex held. A client can only run its first pending command. */
static int first_pending_for_client(int index)
{
    unsigned long key = command_buffer[index]->key;
    for (int i = 0; i < index; i++) {
        if (command_buffer[i]->key == key) {
            return 0;
        }
    }
    return 1;
}

static int next_command_index(void)
{
    int oldest_ready = -1;
    for (int i = 0; i < command_count; i++) {
        command_t *cmd = command_buffer[i];
        if (client_has_active_command(cmd->key)) {
            continue;
        }
        if (oldest_ready == -1) {
            oldest_ready = i;
        }
        if (cmd->cid == PUBLISH && first_pending_for_client(i)) {
            return i;
        }
    }
    return oldest_ready;
}

static command_t *dequeue_command(int executor_id)
{
    pthread_mutex_lock(&queue_mutex);

    int index;
    while ((index = next_command_index()) == -1) {
        pthread_cond_wait(&work_available, &queue_mutex);
    }

    command_t *cmd = command_buffer[index];
    active_commands[executor_id] = cmd;
    command_count--;
    memmove(&command_buffer[index], &command_buffer[index + 1],
            (command_count - index) * sizeof command_buffer[0]);
    pthread_cond_signal(&not_full);
    pthread_mutex_unlock(&queue_mutex);
    return cmd;
}

static void finish_command(int executor_id)
{
    pthread_mutex_lock(&queue_mutex);
    active_commands[executor_id] = NULL;
    /* Clearing the slot may make another command for this client runnable. */
    pthread_cond_signal(&work_available);
    pthread_mutex_unlock(&queue_mutex);
}

static void display_help(char *exec)
{
    printf("Usage: %s -p port_number -r [activate_random_delays]\n", exec);
}


static int parse_command(char* str, command_t *cmd)
{
    char *name = NULL;
    
    /* start by cleaning the input */
    str_clean(str);
    
    /* get command id */
    cmd->cid=str_to_command(str, &cmd->answer_expected);
    
    switch(cmd->cid){
    case LOGIN:
        if(str_to_payload(str, cmd->msg, BABBLE_ID_MSIZE)){
            name = get_name_from_key(cmd->key);
            fprintf(stderr,"Error from [%s]-- invalid LOGIN -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case PUBLISH:
        if(str_to_payload(str, cmd->msg, sizeof cmd->msg)){
            name = get_name_from_key(cmd->key);
            fprintf(stderr,"Warning from [%s]-- invalid PUBLISH -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case FOLLOW:
        if(str_to_payload(str, cmd->msg, BABBLE_ID_MSIZE)){
            name = get_name_from_key(cmd->key);
            fprintf(stderr,"Warning from [%s]-- invalid FOLLOW -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case TIMELINE:
        cmd->msg[0]='\0';
        break;
    case FOLLOW_COUNT:
        cmd->msg[0]='\0';
        break;
    case RDV:
        cmd->msg[0]='\0';
        break;    
    default:
        name = get_name_from_key(cmd->key);
        fprintf(stderr,"Error from [%s]-- invalid client command -> %s\n", name, str);
        free(name);
        return -1;
    }

    return 0;
}


/* processes the command and eventually generates an answer */
static int process_command(command_t *cmd, answer_t **answer)
{
    int res=0;

    switch(cmd->cid){
    case LOGIN:
        res = run_login_command(cmd, answer);
        break;
    case PUBLISH:
        random_delay(random_delay_activated);
        res = run_publish_command(cmd, answer);
        break;
    case FOLLOW:
        random_delay(random_delay_activated);
        res = run_follow_command(cmd, answer);
        break;
    case TIMELINE:
        random_delay(random_delay_activated);
        res = run_timeline_command(cmd, answer);
        break;
    case FOLLOW_COUNT:
        res = run_fcount_command(cmd, answer);
        break;
    case RDV:
        res = run_rdv_command(cmd, answer);
        break;
    case UNREGISTER:
        res = unregisted_client(cmd);
        *answer = NULL;
        break;
    case PARSE_ERROR:
        res = notify_parse_error(cmd, cmd->msg, answer);
        break;
    default:
        fprintf(stderr,"Error -- Unknown command id\n");
        return -1;
    }

    if(res){
        fprintf(stderr,"Error -- Failed to run command ");
        display_command(cmd, stderr);
    }

    return res;
}

static void *executor_thread(void *arg)
{
    int executor_id = *(int *)arg;
    fastRandomSetSeed(time(NULL) + pthread_self() * 100);

    while (1) 
    {
        command_t *cmd = dequeue_command(executor_id);
        answer_t *answer = NULL;

        process_command(cmd, &answer);

        if (answer != NULL && send_answer_to_client(answer) == -1) 
        {
            fprintf(stderr, "Could not send reply to client %lu\n", cmd->key);
        }

        free_answer(answer);
        finish_command(executor_id);
        free(cmd);
    }

    return NULL;
}

static void *communication_thread(void *arg)
{
    int newsocketfd = *(int*)arg;
    free(arg);
    char *recv_buff = NULL;
    int recv_size = network_recv(newsocketfd, (void **)&recv_buff);

    if (recv_size <= 0) 
    {
        free(recv_buff);
        close(newsocketfd);
        return NULL;
    }
    command_t *cmd = new_command(0);

    if (parse_command(recv_buff, cmd) == -1 || cmd->cid != LOGIN) 
    {
        fprintf(stderr, "Error: invalid LOGIN message\n");
        free(recv_buff);
        free(cmd);
        close(newsocketfd);
        return NULL;
    }

    free(recv_buff);
    cmd->sockfd = newsocketfd;
    answer_t *answer = NULL;

    if (process_command(cmd, &answer) == -1) {
        free_answer(answer);
        free(cmd);
        close(newsocketfd);
        return NULL;
    }

    unsigned long client_key = cmd->key;
    int send_result = send_answer_to_client(answer);
    free_answer(answer);

    if (send_result == -1) {
        cmd->cid = UNREGISTER;
        enqueue_command(cmd);
        return NULL;
    }

    free(cmd);
    while ((recv_size = network_recv(newsocketfd, (void **)&recv_buff)) > 0) 
    {
        cmd = new_command(client_key);

        if (parse_command(recv_buff, cmd) == -1 || cmd->cid == LOGIN)
        {
            fprintf(stderr, "Invalid command from client %lu\n", client_key);
            cmd->cid = PARSE_ERROR;
            snprintf(cmd->msg, sizeof cmd->msg, "%s", recv_buff);
        }

        free(recv_buff);
        enqueue_command(cmd);
    }
    free(recv_buff);
    cmd = new_command(client_key);
    cmd->cid = UNREGISTER;
    enqueue_command(cmd);
    return NULL;

}

int main(int argc, char *argv[])
{
    int sockfd, newsockfd;
    int portno=BABBLE_PORT;
    
    int opt;
    int nb_args=1;
    
    while ((opt = getopt (argc, argv, "+hp:r")) != -1){
        switch (opt){
        case 'p':
            portno = atoi(optarg);
            nb_args+=2;
            break;
        case 'r':
            random_delay_activated=1;
            nb_args+=1;
            break;
        case 'h':
        case '?':
        default:
            display_help(argv[0]);
            return -1;
        }
    }
    
    if(nb_args != argc){
        display_help(argv[0]);
        return -1;
    }

    server_data_init();

    pthread_t executors[BABBLE_EXECUTOR_THREADS];
    int executor_ids[BABBLE_EXECUTOR_THREADS];
    for (int i = 0; i < BABBLE_EXECUTOR_THREADS; i++) {
        executor_ids[i] = i;
        int err = pthread_create(&executors[i], NULL, executor_thread, &executor_ids[i]);
        if (err != 0) {
            fprintf(stderr, "Could not start executor: %s\n", strerror(err));
            return EXIT_FAILURE;
        }
    }
    
    if((sockfd = server_connection_init(portno)) == -1){
        return -1;
    }

    printf("Babble server bound to port %d\n", portno);    

    /* main server loop */
    while (1) {
    newsockfd = server_connection_accept(sockfd);
    if (newsockfd == -1) 
    {
        return EXIT_FAILURE;
    }

    int *client_fd = malloc(sizeof *client_fd);
    if (client_fd == NULL) 
    {
        perror("malloc");
        close(newsockfd);
        continue;
    }
    *client_fd = newsockfd;

    pthread_t thread;
    int err = pthread_create(&thread, NULL, communication_thread, client_fd);
    if (err != 0) 
    {
        fprintf(stderr, "Could not start communication thread: %s\n",
                strerror(err));
        free(client_fd);
        close(newsockfd);
        continue;
    }

    pthread_detach(thread);
}
    
    close(sockfd);
    return 0;
}
