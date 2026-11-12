/* Exercise the server's queue directly so thread timing cannot mask ordering. */
#define main babble_server_main
#include "babble_server.c"
#undef main

static void expect_next(int executor_id, command_t *expected)
{
    command_t *actual = dequeue_command(executor_id);
    if (actual != expected) {
        fprintf(stderr, "Executor %d got command %d for client %lu, expected %d for %lu\n",
                executor_id, actual->cid, actual->key, expected->cid, expected->key);
        exit(EXIT_FAILURE);
    }
}

int main(void)
{
    command_t follow = { .cid = FOLLOW, .key = 1 };
    command_t first_publish = { .cid = PUBLISH, .key = 1 };
    command_t rdv = { .cid = RDV, .key = 1 };
    command_t second_publish = { .cid = PUBLISH, .key = 2 };
    command_t third_publish = { .cid = PUBLISH, .key = 2 };

    enqueue_command(&follow);
    enqueue_command(&first_publish);
    enqueue_command(&rdv);
    enqueue_command(&second_publish);

    expect_next(0, &second_publish);
    enqueue_command(&third_publish);
    expect_next(1, &follow);
    finish_command(0);

    expect_next(0, &third_publish);
    finish_command(0);
    finish_command(1);
    expect_next(0, &first_publish);
    finish_command(0);
    expect_next(1, &rdv);
    finish_command(1);

    puts("PASS: PUBLISH priority and per-client order");
    return EXIT_SUCCESS;
}
