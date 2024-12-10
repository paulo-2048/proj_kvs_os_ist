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

int executeCommand(char *command, int fdOut, int fdIn, char *inputFilename)
{
  char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
  unsigned int delay;
  size_t num_pairs;

  // printf("Command: %s\n", command);

  fflush(stdout);

  // Create a temporary file
  char temp_filename[] = "/tmp/command_XXXXXX";
  int fdTempFile = mkstemp(temp_filename);
  if (fdTempFile == -1)
  {
    perror("mkstemp");
    return -1;
  }

  // Write command to temp file
  write(fdTempFile, command, strlen(command));
  write(fdTempFile, "\n", 1);
  lseek(fdTempFile, 0, SEEK_SET);

  switch (get_next(fdTempFile))
  {
  case CMD_WRITE:
    num_pairs = parse_write(fdTempFile, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
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
    num_pairs = parse_read_delete(fdTempFile, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

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
    num_pairs = parse_read_delete(fdTempFile, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

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
    if (parse_wait(fdTempFile, &delay, NULL) == -1)
    {
      fprintf(stderr, "Invalid command. See HELP for usage\n");
    }

    if (delay > 0)
    {
      // fprintf("Waiting...\n");
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
      printf("Backup completed with status: %d\n", status);
    }

    int pid = fork();
    if (pid == 0) // Child process
    {
      pthread_mutex_unlock(&backup_mutex);

      printf("Performing backup...\n");

      // Perform backup
      int backup_result = kvs_backup(fdIn, inputFilename, dirp);

      if (backup_result != 0)
      {
        fprintf(stderr, "Failed to perform backup.\n");
        exit(1);
      }

      // sem_post(&backup_semaphore); // Release semaphore for completed child
      printf("Backup completed.\n");
      exit(0);
    }
    else if (pid > 0) // Parent process
    {
      concurrent_backups++;
      pthread_mutex_unlock(&backup_mutex);

      // Wait for child process to complete
      // int status;
      // pid = wait(&status);

      // sem_post(&backup_semaphore); // Release semaphore for completed child
    }
    else
    {
      // Fork failed
      perror("Fork failed");
      // sem_post(&backup_semaphore);
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
        "  BACKUP\n" // Not implemented
        "  HELP\n");

    break;

  case CMD_EMPTY:
    break;

  case EOC:
    kvs_terminate();
    return 0;
  }

  close(fdTempFile);
  unlink(temp_filename);

  return 0;
}

int readLine(char *filePath)
{
  int fd;
  int fdOut;
  ssize_t bytesRead;
  char buffer[MAX_LINE_LENGTH];
  char line[MAX_LINE_LENGTH];
  int lineIndex = 0;

  // Open the input file using system call
  fd = open(filePath, O_RDONLY);
  if (fd == -1)
  {
    printf("Error opening file %s\n", filePath);
    return -1;
  }

  // Generate output filename
  size_t len = strlen(filePath);
  char outFilename[len + 1];
  generateOutFilename(filePath, outFilename);

  fdOut = open(outFilename, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  if (fdOut == -1)
  {
    printf("Error creating file %s\n", outFilename);
    return -1;
  }

  printf("Executing file: %s\n", filePath);

  while ((bytesRead = read(fd, buffer, sizeof(buffer))) > 0)
  {
    for (int i = 0; i < bytesRead; i++)
    {
      // Process each character
      if (buffer[i] == '\n' || lineIndex >= MAX_LINE_LENGTH - 1)
      {
        // Complete line found
        line[lineIndex] = '\0';

        // Ignore comments (lines starting with #)
        if (lineIndex > 0 && line[0] != '#')
        {
          // printf("Processing line: %s\n", line);
          executeCommand(line, fdOut, fd, filePath);
        }

        // Reset line buffer
        lineIndex = 0;
      }
      else
      {
        // Accumulate characters in line buffer
        line[lineIndex++] = buffer[i];
      }
    }
  }

  // Handle any read errors
  if (bytesRead == -1)
  {
    printf("Error reading file %s\n", filePath);
    close(fd);
    return -1;
  }

  // Handle last line if not terminated by newline
  if (lineIndex > 0 && line[0] != '#')
  {
    line[lineIndex] = '\0';
    // printf("Processing line: %s\n", line);
    executeCommand(line, fdOut, fd, filePath);
  }

  // Close file descriptor
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
  for (;;)
  {

    pthread_mutex_lock(&thread_mutex);
    dp = readdir(dirp);
    pthread_mutex_unlock(&thread_mutex);

    if (dp == NULL)
      break;
    if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0 || strstr(dp->d_name, ".out") != NULL || strstr(dp->d_name, ".bck") != NULL)
      continue;

    // Construct full path to the job file
    char filePath[MAX_JOB_FILE_NAME_SIZE];
    snprintf(filePath, sizeof(filePath), "%s/%s", folderName, dp->d_name);

    pthread_mutex_lock(&thread_mutex);
    concurrent_threads++;
    pthread_mutex_unlock(&thread_mutex);

    readLine(filePath);

    pthread_mutex_lock(&thread_mutex);
    concurrent_threads--;
    pthread_mutex_unlock(&thread_mutex);
  }

  pthread_exit(NULL);
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
  printf("Max Concurrent threads: %d\n", MAX_CONCURRENT_THREADS);

  pthread_t threads[MAX_CONCURRENT_THREADS];

  dirp = opendir(argv[1]);
  if (dirp == NULL)
  {
    perror("Error opening job directory");
    return 1;
  }

  // TODO por o readdir dentro de funcao
  // ligar MAX THREADS com essa func
  // join a todas

  for (int i = 0; i < MAX_CONCURRENT_THREADS; i++)
  {
    if (pthread_create(&threads[i], NULL, read_line_thread, NULL) != 0)
    {
      perror("Failed to create thread");
      break;
    }
  }

  // for (;;)
  // {
  //   while (concurrent_threads >= MAX_CONCURRENT_THREADS)
  //   {
  //     pthread_join(threads[0], NULL);
  //   }

  //   // pass dirp and argv[1] to thread
  //   pthread_create(&threads[concurrent_threads], NULL, read_line_thread, dirp);
  // }

  // Wait for all threads to complete
  for (int i = 0; i < MAX_CONCURRENT_THREADS; i++)
  {
    printf("Joining thread %d [in main]\n\n", i);
    pthread_join(threads[i], NULL);
  }

  // sem_destroy(&backup_semaphore);
  closedir(dirp);
  return 0;
}
