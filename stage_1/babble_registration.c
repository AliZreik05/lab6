#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "babble_registration.h"

client_bundle_t *registration_table[MAX_CLIENT];
static pthread_mutex_t registration_lock = PTHREAD_MUTEX_INITIALIZER;
int nb_registered_clients;


void registration_init(void)
{
    nb_registered_clients=0;

    memset(registration_table, 0, MAX_CLIENT * sizeof(client_bundle_t*));
}

client_bundle_t *registration_lookup(unsigned long key)
{
    client_bundle_t *c = NULL;

    pthread_mutex_lock(&registration_lock);

    for (int i = 0; i < nb_registered_clients; i++) {
        if (registration_table[i]->key == key) {
            c = registration_table[i];
            break;
        }
    }

    pthread_mutex_unlock(&registration_lock);
    return c;
}

int registration_insert(client_bundle_t* cl)
{    
    pthread_mutex_lock(&registration_lock);
    if(nb_registered_clients == MAX_CLIENT){
        pthread_mutex_unlock(&registration_lock);
        fprintf(stderr, "ERROR: MAX NUMBER OF CLIENTS REACHED\n");
        return -1;
    }
    
    /* lookup to find if key already exists */
    int i=0;

    
    for (i = 0; i < nb_registered_clients; i++) {
    if (registration_table[i]->key == cl->key) {
        break;
    }
}

if (i != nb_registered_clients) {
    pthread_mutex_unlock(&registration_lock);
    fprintf(stderr, "Error -- id %lu already in use\n", cl->key);
    return -1;
}

    /* insert cl */
    registration_table[nb_registered_clients]=cl;
    nb_registered_clients++;
    
    pthread_mutex_unlock(&registration_lock);
    return 0;
}


client_bundle_t *registration_remove(unsigned long key)
{
    int i = 0;

    pthread_mutex_lock(&registration_lock);

    for (i = 0; i < nb_registered_clients; i++) {
        if (registration_table[i]->key == key) {
            break;
        }
    }

    if (i == nb_registered_clients) {
        pthread_mutex_unlock(&registration_lock);
        fprintf(stderr, "Error -- no client found\n");
        return NULL;
    }

    client_bundle_t *cl = registration_table[i];

    nb_registered_clients--;
    registration_table[i] = registration_table[nb_registered_clients];
    registration_table[nb_registered_clients] = NULL;

    pthread_mutex_unlock(&registration_lock);
    return cl;
}