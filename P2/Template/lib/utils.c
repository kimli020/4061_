#include "utils.h"
#define PERM  0666

//  User-defined function prototypes
char *getChunkData(int mapperID) {

    /*open Message Queue*/
    key_t key = ftok("project",4285922);          //the key can be whatever, but it has to be the same as the one used to open the msg queue in  sendChunkData(). TA recommends the 2nd argument be student id#
    int mid = msgget(key, PERM | IPC_CREAT);     
    if(mid == -1){
        perror("Error opening msg queue in getChunkData \n");
        exit(-1);
    }
    
  /*Create Buffer for ChunkData*/
    char *retChunk = (char *)malloc(sizeof(char)*chunkSize); //initialize a buffer 
    if(retChunk == NULL) {
      printf("getChunkData malloc() failure \n");
      exit(0);
    }
    memset(retChunk, '\0', chunkSize);
    
    /*Recieve a chunk from the master*/
    struct msgBuffer msg; 
    if(msgrcv(mid, &msg, sizeof(msg.msgText), mapperID, 0) < 0){
      printf("Unable to recieve msg from master in getChunkData\n ");
      exit(0);
    }

    /*copy recieved chunk into previously allocated buffer*/
    strcpy(retChunk, msg.msgText);
      
    /*check for END*/ 
    if(strcmp(msg.msgText, "END") == 0){ //END message received,
      free(retChunk);
      return NULL; 
    }else{
      return retChunk;
    }

}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
  printf("now in sendChunkData... \n");
  
  /*open Message Queue*/
  key_t key = ftok("project", 4285922);        //has to be the same key as the one in getChunkdata()
  int msgid = msgget(key, PERM | IPC_CREAT);
 	if( msgid < 0){
		printf("Failed to open queue in sendChunkData\n");
		exit(0);
	}
  
  /*Open file to read words from*/
  FILE* f = fopen(inputFile, "r"); 
  if(f == NULL){
    printf("Error opening file in SendChunkData\n");
    exit(1);
  }
  
  /*Make and Blank chunk*/
  struct msgBuffer chunk; 
  memset((void *)chunk.msgText, '\0',1024); 
  
  
   /*Scan through file, adding words to chunk and send message as needed*/
 	int totalbytes = 0;                 //running total of how many bytes have currently been read
  int wordlength;                     //how many bytes a word is. Used to let us know if a word would be split if it was added to the chunk 
  char currentword [50];            //used to store the next word in the file 
  int mapperid = 1;                   //start the mapperid at 1 and increment to n
 
  while(fscanf(f,"%s",currentword) !=EOF ){ 
      wordlength = strlen(currentword);                              //store size of the word that was just read from file 
      printf("total bytes  = %d\n", totalbytes);
      if (totalbytes+wordlength+1 <= 1024){                            //the chunk isn't 1024 bytes yet, and wouldn't be if the next word was added
       totalbytes += (wordlength + 1);                                 //+1 for whitespace
      }else{                                                           //the chunk would overflow, splitting a word, so send what is currenty in the chunk
      chunk.msgType = mapperid;
      
      /*Send chunk to queue*/
      int check = msgsnd(msgid, &chunk, sizeof(chunk.msgText), 0);    //WHY DOES THIS LINE HANG???!!!!!!!
      printf("this is the line past send msg...\n");
      if (check == -1){
        perror("this sucks\n");
        exit(0);
      }
      
      /*reset after sending chunk*/
      memset(chunk.msgText, '\0', 1024); 
      totalbytes = (wordlength + 1); 
      
      mapperid += 1;
      if(mapperid > nMappers){
        mapperid = 1;
      }

    }
    
   /*Add the word that was just read from the file into the chunk*/
    strcat(chunk.msgText, strcat(currentword, " ")); 
  }
  
  /*send the last chunk (if there is anything in it) once the file has been fully read*/
  if(totalbytes > 0){
    printf("total bytes = %d\n", totalbytes);
    chunk.msgType = mapperid; 
       
     /*Send chunk to queue*/
    int check1 = msgsnd(msgid, &chunk,sizeof(chunk.msgText),0);
    if (check1 == -1){
        perror("Message send failed\n");
        exit(0);
    }
      
      /*reset chunk after sending it*/
      memset(chunk.msgText, '\0', 1024);
      totalbytes = 0;
  }

  /*send END message to mappers*/ 
  for(int i = 1 ; i < nMappers + 1; i++){
      printf("loop sending end message\n");
      chunk.msgType = i;
      memset(chunk.msgText, '\0', 1024);
      strcpy(chunk.msgText, "END");
      int check2 = msgsnd(msgid, &chunk,sizeof(chunk.msgText), 0) ;
      if(check2== -1){
        perror("Message send failed\n");
        exit(0);
      }

  }

  /*Close everything*/
  fclose(f); 
  msgctl(msgid, IPC_RMID, NULL);
}

// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

void shuffle(int nMappers, int nReducers) {
  
  key_t key = ftok("project", 4285922);  
  int msgid = msgget(key, PERM | IPC_CREAT );
  if (msgid == -1){
   perror("Failed to create message queue in shuffle\n");
   exit(0);
  }
  
	struct msgBuffer msg;
	char *wordFilePath;
	char *wordFileName;
	int reducerID = 0;

	//traverse the directory of each Mapper and send the word filepath to the reducers
	for(int i = 1; i <= nMappers; i += 1)
	{
      		//have mapid increment to be correct directory name to open
      		char mapid_str[100];
      		int map_id = i;
      		sprintf(mapid_str, "output/MapOut/Map_%i", map_id);

		printf("path: %s\n", mapid_str);
		fflush(stdout);

		DIR *dir = opendir(mapid_str);

		struct dirent *dir_entry;

     		char curr_dir[1024] = {'\0'};
      		getcwd(curr_dir, sizeof(curr_dir));

		//select reducer id and send word filepath to reducer
		while((dir_entry = readdir(dir)) != NULL)
		{

			if ((strcmp(dir_entry->d_name, ".") == 0) || (strcmp(dir_entry->d_name, "..") == 0))
			{
				continue;
			}

			wordFileName = dir_entry->d_name;

			printf("wordFileName: %s\n", wordFileName);
			fflush(stdout);

			//select reducer
                        reducerID = hashFunction(wordFileName, nReducers) + 1;

			//check if regular file and concatenate to get wordFilePath
        		if((dir_entry->d_type == DT_REG))
			{
          			struct stat entry_stat;
          			char this_entry[1024] = {'\0'};

				strcat(this_entry, curr_dir);
          			strcat(this_entry, "/");
          			strcat(this_entry, mapid_str);
				strcat(this_entry, "/");
				strcat(this_entry, dir_entry->d_name);

				//prepare to send word filepath to reducers
				wordFilePath = this_entry;
				msg.msgType = reducerID;
				memset(msg.msgText, '\0', MSGSIZE);
				strcpy(msg.msgText, wordFilePath);
			}

			//send word filepath to reducers
			if( (msgsnd(msgid, &msg, sizeof(msg.msgText), 0)) < 0)
			{
				printf("message filepath send error\n");
				exit(0);
			}

		}


		printf("outside of shuffle while loop\n");
		fflush(stdout);

		closedir(dir);
		continue;
	}

	//send END message to reducers
	for (int j = 1; j <= nReducers; j += 1) { 
		msg.msgType = (j % nReducers) + 1;
		memset(msg.msgText, '\0', MSGSIZE);
		strcpy(msg.msgText, "END");

		if (msgsnd(msgid, &msg, sizeof(msg.msgText), 0) < 0) {
			printf("message send error\n");
			exit(0);
		}

	}
}

int getInterData(char *key, int reducerID) {  //key is the file path to txt file

  /*open Message Queue*/
  key_t MsgKey = ftok("project",4285922);          //the key can be whatever, but it has to be the same as the one used to open the msg queue in  sendChunkData(). TA recommends the 2nd argument be student id#
  int mid = msgget(MsgKey, PERM | IPC_CREAT);      //User, groups and other have R/W. Create queue if it doesn't already exist
  if(mid == -1){
      perror("Error opening msg queue in getInterData \n");
      exit(-1);
  }

  /*Make and Blank chunk*/
  struct  msgBuffer chunk;
  memset((void *)chunk.msgText, '\0',1024); 
  
  /*Recieve from queue*/
  int rcv = msgrcv(mid,(void *)&chunk, sizeof(chunk.msgText), reducerID, 0);
  if (rcv == -1){
    perror("Failed to get recieve from Queue\n");
    exit(0);

  }
  
   char*c = malloc(sizeof(chunk.msgText));
   strcpy(c, chunk.msgText); 
   strcpy(key, c);// copy recieved chunk to key
  
    if (strcmp(key,"END") == 0){
        return 0;
    }
    
  return 1; 
}



/*
  ****************************************************************************************************************************************
  Functions given in code template  ******************************************************************************************************
  ****************************************************************************************************************************************
*/

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char)*chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
