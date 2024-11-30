#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"

char *generateOutFilename(char *filename, char *outFilename)
{
  size_t len = strlen(filename);
  strcpy(outFilename, filename);
  outFilename[len - 4] = '\0';
  strcat(outFilename, ".out");

  return outFilename;
}

int executeCommand(char *command, int fdOut)
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

    if (kvs_backup())
    {
      fprintf(stderr, "Failed to perform backup.\n");
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

  printf("Executing command: %s\n", filePath);

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
          executeCommand(line, fdOut);
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
    executeCommand(line, fdOut);
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

int main(int argc, char *argv[])
{

  if (argc != 2)
  {
    fprintf(stderr, "Usage: %s <job_directory>", argv[0]);
    return 1;
  }

  if (kvs_init())
  {
    printf("Failed to initialize KVS\n");
    return 1;
  }

  DIR *dirp = opendir(argv[1]);
  if (dirp == NULL)
  {
    perror("Error opening job directory");
    return 1;
  }

  struct dirent *dp;
  for (;;)
  {
    dp = readdir(dirp);
    if (dp == NULL)
      break;
    if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0 || strstr(dp->d_name, ".out") != NULL)
      continue; /* Skip . and .. */

    // Construct full path to the job file
    char filePath[PATH_MAX];
    snprintf(filePath, sizeof(filePath), "%s/%s", argv[1], dp->d_name);

    readLine(filePath);
  }

  closedir(dirp);
  return 0;
}
