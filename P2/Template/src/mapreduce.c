#include "mapreduce.h"
#define PERM 0666

// execute executables using execvp
void execute(char **argv, int nProcesses){
	pid_t  pid;

	int i;
	for (i = 0; i < nProcesses; i++){
		pid = fork();
		if (pid < 0) {
			printf("ERROR: forking child process failed\n");
			exit(1);
		} else if (pid == 0) {
			char *processID = (char *) malloc(sizeof(char) * 5); // memory leak
			sprintf(processID, "%d", i+1);
			argv[1] = processID;
			if (execvp(*argv, argv) < 0) {
				printf("ERROR: exec failed\n");
				exit(1);
			}
		}
     }
}

int main(int argc, char *argv[]) {

	if(argc < 4) {
		printf("not enough arguments.\n");
		printf("./mapreduce #mappers #reducers inputFile\n");
		exit(0);
	}

	int nMappers 	= strtol(argv[1], NULL, 10);
	int nReducers 	= strtol(argv[2], NULL, 10);

	if(nMappers < nReducers){
		printf("ERROR: Number of mappers should be greater than or equal to number of reducers...\n");
		exit(0);
	}

	if(nMappers == 0 || nReducers == 0){
		printf("ERROR: Mapper and Reducer count should be greater than zero...\n");
		exit(0);
	}


	char *inputFile = argv[3];

	bookeepingCode();

	int status;
	pid_t pid = fork();
	if(pid == 0){
		//send chunks of data to the mappers in RR fashion
		sendChunkData(inputFile, nMappers);
		exit(0);
	}
	sleep(1);

	// spawn mappers
	char *mapperArgv[] = {"./mapper", NULL, NULL};
	execute(mapperArgv, nMappers);

	// wait for all children to complete execution
    while (wait(&status) > 0);
    
		/* Wait for ACK from mappers - until have received all messages of type ACK */
        pid = fork();
        if(pid == 0){
            printf("Got to shuffle in mapreduce... \n");
            shuffle(nMappers, nReducers);
            exit(0);
        }
        sleep(1);
        char *reducerArgv[] = {"./reducer", NULL, NULL};
        execute(reducerArgv, nReducers);


	// wait for all children to complete execution
    while (wait(&status) > 0);
    
        key_t MsgKey = ftok("project",4285922);
        int msgid = msgget(MsgKey, PERM | IPC_CREAT);
        if(msgid == -1){
            perror("Failed to get Queue");
            exit(0); 
        }
   
        if (msgctl(msgid, IPC_RMID, NULL) < 0){
            printf("Error: could not close message queue\n");
            return -1;
        }

            return 0;
}
