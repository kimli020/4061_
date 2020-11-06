#include "utils.h"
#define PERM  0666
#define PATHSIZE 50
#define ENDMSG "END\0"
#define ACKMSG "ACK\0"
/*Define other message structs needed*/
struct msgBuffer2 {                  //for sending and recieving chunks of data
    long msgType;
    char msgText[chunkSize+1];
};
 
struct msgBuffer3{                  //for sending and recieving filepaths
  long msgType;
  char msgText[PATHSIZE+1];
};



//  User-defined function prototypes
char *getChunkData(int mapperID) {
    
    printf("ChunkData checkpoint 1. mapperID = %d\n", mapperID);
    
    /*open Message Queue*/
    key_t key = ftok("project", 4285922);          //the key can be whatever, but it has to be the same as the one used to open the msg queue in  sendChunkData(). TA recommends the 2nd argument be student id#
    int mid = msgget(key, PERM | IPC_CREAT);     
    if(mid == -1){
        perror("Error opening msg queue in getChunkData \n");
        exit(-1);
    }
    
    /*Recieve a chunk from the master*/
    struct msgBuffer2 msg1; 
    memset((void *)msg1.msgText, '\0',sizeof(msg1.msgText)); 
    int valid =  msgrcv(mid, &msg1, sizeof(msg1.msgText), mapperID, 0);
    if(valid == -1){
      printf("Unable to recieve msg from master in getChunkData\n ");
      exit(0);
    }
  
    /*Create Buffer for chunk recieved from master*/
    char *retChunk;
    retChunk = (char *)malloc(sizeof(char)*(chunkSize+1));
    memset(retChunk, '\0', chunkSize+1); 
 
 
     /*copy recieved chunk into previously allocated buffer*/
    strcpy(retChunk, msg1.msgText);
    printf("ChunkData checkpoint 2. mapperID = %d\n", mapperID);

    /*check for END / Send ACK*/ 
    if(strcmp(msg1.msgText, ENDMSG) == 0){
        printf("End msg recieved in getChunkData() for mapperID %d\n", mapperID);
    	  return retChunk; 
    }else{
      struct msgBuffer2 ackm;
      ackm.msgType = ACKTYPE;
      strcpy(ackm.msgText, ACKMSG);
      int send = msgsnd(mid, &ackm, sizeof(struct msgBuffer2), 0);
      if ( send == -1) { // send an ack message 
        printf("failed to send ACK mesg in getChunkData() for mapperID %d\n", mapperID);
        exit(0);
      }
    }
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
   printf("sendChunkData checkpoint 1. mapperID = %d\n", mapperID);
  
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
  struct msgBuffer2 chunk; 
  memset((void *)chunk.msgText, '\0',1024); 
  
  
   /*Scan through file, adding words to chunk and send message as needed*/
 	int totalbytes = 0;                 //running total of how many bytes have currently been read
  int wordlength;                     //how many bytes the currently read word is. Used to let us know if a word would be split if it was added to the chunk 
  char currentword [50];              //used to store the word being read from the file 
  int mapperid = 1;                   //keep track of what mapper to send chunk to. start at 1 and increment to n
 
  while(fscanf(f,"%s",currentword) !=EOF ){ 
      wordlength = strlen(currentword);                                //store size of the word that was just read from file 
      if ((totalbytes + wordlength + 1) <= chunkSize){                  //the chunk isn't 1024 bytes yet, and wouldn't be if the next word was added
        totalbytes += (wordlength + 1);                                 //+1 for whitespace
      }else{                                                           //the chunk would overflow, splitting a word, so send what is currenty in the chunk
      chunk.msgType = mapperid;
      printf("chunk.msgType = %ld\n",  chunk.msgType);
      printf("size of message = %ld bytes\n",  strlen(chunk.msgText));
      
      /*Send chunk to queue*/
      printf("sending in sendChunkData: %s\n", chunk.msgText);
      int check = msgsnd(msgid, &chunk, sizeof(chunk.msgText), 0);    //WHY DOES THIS LINE HANG???!!!!!!!
      if (msgsnd(msgid, &chunk, sizeof(chunk.msgText), 0) == -1){
        printf("Error sending file in SendChunkData for mapperID# %d\n", mapperid);
        exit(0);
      }
      
      /*reset after sending chunk*/
      memset(chunk.msgText, '\0', 1024); 
      totalbytes = (wordlength + 1); 
    
    }
    
   /*Add the word that was just read from the file into the chunk*/
    strcat(chunk.msgText, strcat(currentword, " ")); 
  }
  
  /*send the last chunk (if there is anything in it) once the file has been fully read*/
  if(totalbytes > 0){
    printf("sending last chunk");
    chunk.msgType = mapperid; 
       
     /*Send chunk to queue*/
     printf("sending in sendChunkData: %s\n", chunk.msgText);
    int check1 = msgsnd(msgid, &chunk,sizeof(chunk.msgText),0);
    if (check1 == -1){
        perror("Message send failed\n");
        exit(0);
    }
      
      /*reset chunk after sending it*/
      memset(chunk.msgText, '\0', 1024);
      totalbytes = 0;
  }
  
  /*Ensure RR fashion:*/
    mapperid += 1;
    if(mapperid > nMappers){
        mapperid = 1;
    }

  /*send END msg to mappers*/ 
  memset(chunk.msgText, '\0', 1024);
  strcpy(chunk.msgText, ENDMSG);
  for(int i = 1 ; i < nMappers + 1; i++){
      printf("loop sending end message\n");
      chunk.msgType = i;
       if(msgsnd(msgid, &chunk,sizeof(chunk.msgText), 0) == -1){
        perror("END Message send failed\n");
        exit(0);
      }
  }
  /* wait for ACK from mappers*/
  for(int i = 0; i < nMappers; i++){
    if (msgrcv(msgid, &chunk, sizeof(struct msgBuffer), ACKTYPE, 0) == -1){
      printf("Failed to receive ack message in sendChunkData()\n");
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
  
  printf("Now in shuffle\n");
   
  /*Message queue*/
  key_t key = ftok("project", 4285922);  
  int msgid = msgget(key, PERM | IPC_CREAT );
  if (msgid == -1){
    perror("Failed to create message queue in shuffle\n");
    exit(0);
  }
  
  
   /*Directory Traversal*/
  struct dirent* entry;
  for(int i=1; i<nMappers+1; i++) {
    
    /*Variables needed*/
    int reducerID;
    char path[50] = "output/MapOut/Map_"; // path of file will never be more than 50
    char strnum[5];
    sprintf(strnum,"%d",i);
    strcat(path,strnum); 
      
    DIR* dire = opendir(path);
    struct dirent* entry;
    while ((entry = readdir(dire)) != NULL) {
     
     /*Do nothing for these kinds of files*/
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")){  
        continue;
    }
      
       /*generate file path*/
      struct msgBuffer3 chunk1;
      memset(chunk1.msgText, '\0', 1024);
      char filepath[50] =""; 
      strcpy(filepath, path); 
      strcat(filepath, "/");
      strcat(filepath, entry->d_name); 
      strcat(chunk1.msgText, filepath); 
      
      /*use the given hash function to send file to reducer*/
      reducerID = hashFunction(entry->d_name, nReducers);
      chunk1.msgType = reducerID + 1;
      int test = msgsnd(msgid, (void *)&chunk1,sizeof(chunk1.msgText),0);
      if (test == -1){
        perror("Failed sending message to reducer in Shuffle()\n");
        exit(0);
      } 
    }
  /*Close Directory*/
  closedir(dire);
  }
  

  
  /*Create and send END message to reducers*/ 
  struct msgBuffer2 endm;
  memset(endm.msgText, '\0', sizeof(endm.msgText));
  strcat(endm.msgText, "MSG"); 
  for(int i = 1; i < nReducers + 1; i++){
      endm.msgType = i;
      msgsnd(msgid, (void *)&endm,sizeof(endm.msgText),0);
  }
  
  /* Recieve ACK from reducers */
  for(int i = 0; i < nReducers; i++){
    if (msgrcv(msgid, &endm, sizeof(endm.msgText), ACKTYPE, 0) == -1) {
      printf("failed to receive ack message in shuffle()\n");
      exit(0);
    }
  }
   
}

int getInterData(char *key, int reducerID) {  //key is the file path to txt file
  printf("Now in getInterData\n");

  /*open Message Queue*/
  key_t MsgKey = ftok("project",4285922);          //the key can be whatever, but it has to be the same as the one used to open the msg queue in  sendChunkData(). TA recommends the 2nd argument be student id#
  int mid = msgget(MsgKey, PERM | IPC_CREAT);      //User, groups and other have R/W. Create queue if it doesn't already exist
  if(mid == -1){
      perror("Error opening msg queue in getInterData \n");
      exit(-1);
  }

  /*Make and Blank message and recieve from master*/
  struct  msgBuffer3 chunk;
  memset((void *)chunk.msgText, '\0',1024); 
  chunk.msgType = reducerID;
  int rcv = msgrcv(mid,(void *)&chunk, sizeof(chunk.msgText), reducerID, 0);
  if (rcv == -1){
    perror("Failed to recieve from queue in getInterData\n");
    exit(0);

  }
  /*Recieve END message and send ACK to master*/
  if(!strcmp(chunk.msgText, ENDMSG)){
    chunk.msgType = ACKTYPE;
    memset(chunk.msgText, '\0', PATHSIZE+1);      //max path name is 50
    strcpy(chunk.msgText, ACKMSG);
    if (msgsnd(mid, &chunk, sizeof(struct msgBuffer), 0) == -1) {
      printf("failed to send ack message in getInterData() from reducerID %d\n", reducerID);
      exit(0);
    }
    return 0;
  }else{
    strcpy(key, chunk.msgText); 
    return 1;
  }
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
