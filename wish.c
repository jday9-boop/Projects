#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>

char *PATHS[100] = {""};
int paths_length;

void set_paths(char this_command[]) {
    paths_length = 0;
    for (int i = 0; i < 99; i++) {
        PATHS[i] = "/";
    }
    int j = 0;
    //printf("command before using strtok: %s\n", this_command);
    this_command = strtok(NULL, " ");
    //printf("this_command inside setpaths: %s\n", this_command);
    while (this_command) {
        //char copy[50];
        //strcat(copy, "/");
        //strcat(copy, this_command);
        //printf("PATH: %s\n", copy);
        paths_length++;
        PATHS[j] = this_command;
        //printf("PATHS[%d] in set_paths: %s\n", j, PATHS[j]);
        j++;
        this_command = strtok(NULL, " ");
    }
}

void execute_command(char this_command[]) {
    int k = 0;
    //char *cat;
    char real_path[50];
    int acc;
    //printf("this_command to start execute_command: %s\n", this_command);
    //printf("In execute_command\n");
    //printf("PATH index with no current path: %s\n", PATHS[0]);
    while (k < paths_length) {
        //printf("IN ex_command loop\n");
        char path[50] = "";
        char path1[50];
        //char delim[4] = "/";
        //strcat(path, "/");
        //printf("PATHS[k]: %s\n", PATHS[k]);
        strcat(path, PATHS[k]);
        strcpy(path1, this_command);
        strcat(path, "/");
        strcat(path, path1);
        acc = access(path, X_OK);
        printf("path: %s access: %d\n", path, acc);
        if (acc == 0) {
            strcpy(real_path,path);
            break;
        }
        k++;
    }
    /*if (acc == -1) {
        char error_message[30] = "An error has occurred\n";
        write(STDERR_FILENO, error_message, strlen(error_message));

    }*/
    printf("After while loop\n");
    if (acc != -1) {
        int rc = fork();
        if (rc == 0) {
            char *cmd_argv[10];
            int p = 1;
            cmd_argv[0] = this_command;
            this_command = strtok(NULL, " ");
            if (strcmp(this_command, ">") == 0) {
                if (strtok(NULL, " ") != NULL) {
                    char error_message[30] = "An error has occurred\n";
                    write(STDERR_FILENO, error_message, strlen(error_message));
                }
                else {
                    (void) close(STDOUT_FILENO);
                    this_command = strtok(NULL, " ");
                    open(this_command, O_WRONLY | O_CREAT | O_TRUNC);
                }
            }
            else {
            while (this_command != NULL) {
               cmd_argv[p] = strdup(this_command);
               this_command = strtok(NULL, " ");
               printf("before closing stdout");
               p++;
               if (strcmp(this_command, ">") == 0) {
                    (void) close(STDOUT_FILENO);
                    this_command = strtok(NULL, " ");
                    open(this_command, O_WRONLY | O_CREAT | O_TRUNC);
                    break;
                }
            }
            }
            printf("Before execv call\n");
            cmd_argv[p] = NULL;
            execv(real_path, cmd_argv);
            char error_message[30] = "An error has occurred\n";
            write(STDERR_FILENO, error_message, strlen(error_message));
            //printf("FAILED!\n");
            }
        else if (rc > 0) {
            (void) wait(NULL);
            //printf("EXECUTE COMMAND IS DONE!\n");
        }   
    } else {
        char error_message[30] = "An error has occurred\n";
        write(STDERR_FILENO, error_message, strlen(error_message));
    }
}

int main(int argc, char *argv[]) {
    //printf("wish> ");
    PATHS[0] = "/bin";
    paths_length = 1;
    //printf("ACCESS : %d\n", access("/bin/echo", X_OK));
    char *command;
    char *this_command;
    int file_counter = 1;
    //printf("ARGC: %d\n", argc);
    //int marker = 0;
    size_t line_buf_size = 0;
    FILE *f = fopen(argv[file_counter], "r");
    int firstloop = 1;
    while (strcmp(command, "EOF") != 0) {
        int marker = 0;
        int eof = 0;
        int eof2 = 0;
        //int firstloop = 1;
        //printf("START OF WHILE LOOP\n");
        //FILE *f = fopen(argv[file_counter], "r");
        if (argc > 1 && strcmp(argv[0], "./wish") == 0 && file_counter < argc && firstloop == 1) {
            eof = getline(&command, &line_buf_size, f);
            //eof2 = getline(&command, &line_buf_size, f);
            //printf("GETLINE AFTER 1 CALL: %s EOF: %d\n", command, eof);
            file_counter++;
            firstloop = 0;
            if (eof == -1) {
                char error_message[30] = "An error has occurred\n";
                write(STDERR_FILENO, error_message, strlen(error_message));
                //exit(0);
            }
            //fclose(f);
        } 
        else if (argc > 1 && strcmp(argv[0], "./wish") == 0 && firstloop == 0) {
            //FILE *f = fopen(argv[file_counter], "r");
            eof2 = getline(&command, &line_buf_size, f);
            //eof2 = getline(&command, &line_buf_size, f);
            //printf("GETLINE AFTER 2 CALLS: %s EOF2: %d\n", command, eof2);
            if (eof2 == -1) {
                exit(0);
            }
            //fclose(f);
        }    
        else if (argc == 1 && file_counter < 2) {
            printf("wish> ");
            getline(&command, &line_buf_size, stdin);
        }
        else {
            exit(0);
        } 
        //printf("COMMAND: %s\n", command);
        command[strcspn(command, "\n")] = 0;
        //this_command = strtok(
        this_command = strtok(command, " ");
        /*if (this_command == "EOF") {
            exit(0);
        }*/
        //printf("BEFORE EXECUTION OF COMMANDS - COMMAND: %s THIS_COMMAND: %s\n", command, this_command);
        if (strcmp(this_command, "cd") == 0) {
            this_command = strtok(NULL," ");
            /*if (strcmp(strtok(NULL, " "), "EOF") == 0) {
                getline(&command, &line_buf_size, f);
                this_command = strtok(command, " ");
            }
            else {
                this_command = strtok(NULL, " ");
            }*/
            //printf("THIS COMMAND RIGHT BEFORE CHANGING DIRECTORY: %s\n", this_command);
            if (this_command == NULL) {
                //printf("ERROR CHANGING DIRECTORY!\n");
                char error_message[30] = "An error has occurred\n";
                write(STDERR_FILENO, error_message, strlen(error_message));
                
            }
            else { 
                int chd = chdir(this_command);
                if (chd < 0) {
                    char error_message[30] = "An error has occurred\n";
                    write(STDERR_FILENO, error_message, strlen(error_message));
                
                } //printf("ERROR CHANGING DIRECTORY!\n");
            }
        }
        else if (strcmp(this_command, "path") == 0) {
            set_paths(this_command);
        }
        else if (strcmp(this_command,"exit") == 0) {
            if (strtok(NULL, " ") != NULL) {
                char error_message[30] = "An error has occurred\n";
                write(STDERR_FILENO, error_message, strlen(error_message));  
            } else {
            exit(0);
            }
        }
        else if (strcmp(this_command, "EOF") == 0) {

        }
        else {
            execute_command(this_command);
        }
        //printf("REACHED END OF WHILE LOOP\n");
        if (marker == 1) {
            exit(0);
        }
        /*if (marker == 0) {
            printf("wish> ");
            fgets(command, 100, stdin);
        }*/
        //this_command = strtok(NULL, " ");
        //scanf("%s", command);
    }
    fclose(f);
    exit(0);
   /* for (int i = 1; i < argc; i++) {
        printf("arg: %s\n", argv[i]);
    }*/


    return 0;
}
