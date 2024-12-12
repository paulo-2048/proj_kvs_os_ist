#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <semaphore.h>
#include <pthread.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

char *folderName;
int MAX_CONCURRENT_BACKUPS;
int MAX_CONCURRENT_THREADS;

int concurrent_backups = 0;
int concurrent_threads = 0;

pthread_mutex_t backup_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t thread_mutex = PTHREAD_MUTEX_INITIALIZER;

DIR *dirp;

char *generateOutFilename(char *filename, char *outFilename)
{
  size_t len = strlen(filename);
  strcpy(outFilename, filename);
  outFilename[len - 4] = '\0';
  strcat(outFilename, ".out");

  return outFilename;
}

int executeCommand(int fdOut, int fdIn, char *inputFilename)
{
  char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  unsigned int delay;
  size_t num_pairs;

  while (1)
  {
    switch (get_next(fdIn))
    {
    case CMD_WRITE:
      num_pairs = parse_write(fdIn, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
      }

      if (kvs_write(num_pairs, keys, values))
      {
        fprintf(stderr, "Failed to write pair\n");
      }

      break;

    case CMD_READ:
      num_pairs = parse_read_delete(fdIn, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
      }

      if (kvs_read(num_pairs, keys, fdOut))
      {
        fprintf(stderr, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs = parse_read_delete(fdIn, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
      }

      if (kvs_delete(num_pairs, keys, fdOut))
      {
        fprintf(stderr, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:

      kvs_show(fdOut);
      break;

    case CMD_WAIT:
      if (parse_wait(fdIn, &delay, NULL) == -1)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
      }

      if (delay > 0)
      {
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:

      pthread_mutex_lock(&backup_mutex);

      while (concurrent_backups >= MAX_CONCURRENT_BACKUPS)
      {
        int status;
        printf("Waiting for one backup complete...\n");
        wait(&status);
        concurrent_backups--;
        printf("Backup complete.\n");
      }

      int pid = fork();
      if (pid == 0)
      {
        pthread_mutex_unlock(&backup_mutex);

        int backup_result = kvs_backup(inputFilename, dirp);

        if (backup_result != 0)
        {
          fprintf(stderr, "Failed to perform backup.\n");
          exit(1);
        }

        _exit(0);
      }
      else if (pid > 0)
      {
        concurrent_backups++;
        pthread_mutex_unlock(&backup_mutex);
      }
      else
      {

        perror("Fork failed");
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      printf(
          "Available commands:\n"
          "  WRITE [(key,value)(key2,value2),...]\n"
          "  READ [key,key2,...]\n"
          "  DELETE [key,key2,...]\n"
          "  SHOW\n"
          "  WAIT <delay_ms>\n"
          "  BACKUP\n"
          "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      return 0;
      break;
    }
  }

  return 0;
}

int readLine(char *filePath)
{
  int fd;
  int fdOut;

  fd = open(filePath, O_RDONLY);
  if (fd == -1)
  {
    printf("Error opening file %s\n", filePath);
    return -1;
  }

  size_t len = strlen(filePath);
  char outFilename[len + 1];
  generateOutFilename(filePath, outFilename);

  fdOut = open(outFilename, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  if (fdOut == -1)
  {
    printf("Error creating file %s\n", outFilename);
    return -1;
  }

  executeCommand(fdOut, fd, filePath);

  if (close(fd) == -1)
  {
    printf("Error closing file %s\n", filePath);
    return -1;
  }

  printf("\n");

  close(fdOut);

  return 0;
}

void *read_line_thread()
{
  struct dirent *dp;
  char filePath[MAX_JOB_FILE_NAME_SIZE];

  while (1)
  {

    pthread_mutex_lock(&thread_mutex);
    dp = readdir(dirp);
    pthread_mutex_unlock(&thread_mutex);

    if (dp == NULL)
      break;

    if (strcmp(dp->d_name, ".") == 0 ||
        strcmp(dp->d_name, "..") == 0 ||
        strstr(dp->d_name, ".out") != NULL ||
        strstr(dp->d_name, ".bck") != NULL)
      continue;

    snprintf(filePath, sizeof(filePath), "%s/%s", folderName, dp->d_name);

    printf("Reading file: %s\n", filePath);

    readLine(filePath);
  }

  pthread_exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[])
{

  if (argc != 4)
  {
    fprintf(stderr, "Usage: %s [job_directory] [concurrent_backups] [concurrent_backups]\n", argv[0]);
    return 1;
  }

  if (kvs_init())
  {
    printf("Failed to initialize KVS\n");
    return 1;
  }

  folderName = argv[1];
  printf("Folder name: %s\n", folderName);

  MAX_CONCURRENT_BACKUPS = atoi(argv[2]);
  printf("Max Concurrent backups: %d\n", MAX_CONCURRENT_BACKUPS);

  MAX_CONCURRENT_THREADS = atoi(argv[3]);
  printf("Max Concurrent threads: %d\n\n", MAX_CONCURRENT_THREADS);

  pthread_t threads[MAX_CONCURRENT_THREADS];

  dirp = opendir(argv[1]);
  if (dirp == NULL)
  {
    perror("Error opening job directory");
    return 1;
  }

  for (int i = 0; i < MAX_CONCURRENT_THREADS; i++)
  {
    if (pthread_create(&threads[i], NULL, read_line_thread, NULL) != 0)
    {
      perror("Failed to create thread");
      break;
    }
  }

  for (int i = 0; i < MAX_CONCURRENT_THREADS; i++)
  {
    pthread_join(threads[i], NULL);
  }

  closedir(dirp);
  return 0;
}
