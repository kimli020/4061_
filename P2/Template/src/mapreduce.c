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

  //remove queue at beginning to ensure nothing left in queue
  key_t key = ftok("project", 4285922);
	msgctl(key, IPC_RMID, NULL); 

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
	  printf("Got to send chunkdata in mapreduce... \n");
		sendChunkData(inputFile, nMappers);
    printf("finished chunkdata in mapreduce... \n");
		exit(0);
	}
	sleep(1);

	// spawn mappers
	char *mapperArgv[] = {"./mapper", NULL, NULL};
	execute(mapperArgv, nMappers);

	// wait for all children to complete execution
   while (wait(&status) > 0);
   
   // key_t key = ftok("project", 4285922);
    int mid = msgget(key, PERM|IPC_CREAT);
    if(mid == -1){
      perror("Failed to get Queue ID");
      exit(0); 
    }
     
    /*Delete the queue after sendChunkData*/
    int del1 = msgctl(mid, IPC_RMID, NULL);
    if(del1 == -1){
      perror("Queue deletion failed");
      exit(0); 
    }
    
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
  
    key = ftok("project", 4285922);
    mid = msgget(key,PERM|IPC_CREAT);
    if(mid == -1){
      perror("Failed to get Queue ID");
      exit(0); 
    }

    int del2 = msgctl(mid, IPC_RMID, NULL); // final deletion of queue 
    
    if (del2 == -1){
      perror("Failed to get Queue ID");
      exit(0); 
    }

    if(mid == -1){
      perror("Failed to get Queue ID");
      exit(0); 
    }

	return 0;
}
}