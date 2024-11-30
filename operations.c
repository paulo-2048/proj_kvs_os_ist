#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms)
{
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init()
{
  if (kvs_table != NULL)
  {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate()
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE])
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++)
  {
    if (write_pair(kvs_table, keys[i], values[i]) != 0)
    {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fdOut)
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // printf("[");
  write(fdOut, "[", 1);
  for (size_t i = 0; i < num_pairs; i++)
  {
    char *result = read_pair(kvs_table, keys[i]);
    if (result == NULL)
    {
      // printf("(%s,KVSERROR)", keys[i]);
      write(fdOut, "(", 1);
      write(fdOut, keys[i], strlen(keys[i]));
      write(fdOut, ",KVSERROR)", 10);
    }
    else
    {
      // printf("(%s,%s)", keys[i], result);
      write(fdOut, "(", 1);
      write(fdOut, keys[i], strlen(keys[i]));
      write(fdOut, ",", 1);
      write(fdOut, result, strlen(result));
      write(fdOut, ")", 1);
    }
    free(result);
  }
  // printf("]\n");
  write(fdOut, "]\n", 2);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fdOut)
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++)
  {
    if (delete_pair(kvs_table, keys[i]) != 0)
    {
      if (!aux)
      {
        // printf("[");
        write(fdOut, "[", 1);
        aux = 1;
      }
      // printf("(%s,KVSMISSING)", keys[i]);
      write(fdOut, "(", 1);
      write(fdOut, keys[i], strlen(keys[i]));
      write(fdOut, ",KVSMISSING)", 12);
    }
  }
  if (aux)
  {
    // printf("]\n");
    write(fdOut, "]\n", 2);
  }

  return 0;
}

void kvs_show(int fdOut)
{
  for (int i = 0; i < TABLE_SIZE; i++)
  {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL)
    {
      // printf("(%s, %s)\n", keyNode->key, keyNode->value);
      write(fdOut, "(", 1);
      write(fdOut, keyNode->key, strlen(keyNode->key));
      write(fdOut, ",", 1);
      write(fdOut, keyNode->value, strlen(keyNode->value));
      write(fdOut, ")\n", 2);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup()
{
  return 0;
}

void kvs_wait(unsigned int delay_ms)
{
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}