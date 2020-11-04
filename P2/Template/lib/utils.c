#include "utils.h"

char *getChunkData(int mapperID) {

    /*open Message Queue*/
    key_t key = ftok("project",4285922);          //the key can be whatever, but it has to be the same as the one used to open the msg queue in  sendChunkData(). TA recommends the 2nd argument be student id#
    struct msgBuffer msg, ACKmsg;                        //declare an instance of msgBuffer to represent the 1024-bit chunk
    int mid = msgget(key, PERM | IPC_CREAT);      //User, groups and other have R/W. Create queue if it doesn't already exist
    int ACKsent;

    if(mid == -1){
        perror("Error opening msg queue in getChunkData \n");
        exit(-1);
    }

    char *retChunk = (char *)malloc(sizeof(char)*chunkSize); //initialize a buffer for chunk data - chunk size = 1024 bytes
    if(retChunk = NULL) {
      printf("getChunkData malloc() failure \n");
      exit(0);
    }
    memset(retChunk, '\0', chunkSize);
     //error handling:

    //receive data from the master who was supposed to send a specific mapperID/
    msgrcv(mid, &msg, sizeof(msg.msgText), mapperID, 0);

    if(strcmp(msg.msgText, "END") == 0){ //END message received, send a message of type ACK
      ACKmsg.msgType = ACKTYPE;
      strcpy(ACKmsg.msgText, "ACK");
      ACKsent = msgsnd(msgid, (void*) &msg, sizeof(msg.msgText), 0);
      if (ACKsent < 0) {
        perror("Error sending ACK message");
      }
    }
    else {  //meaningful chunk of data in msg.msgText
      // copy chunk data into retChunk
      memcpy(retChunk, msg.msgText, sizeof(char)*chunkSize);
    }
    // Whatever calls getChunkData should remember to call free()
    return retChunk;
}

// sends chunks of size 1024 to the mappers in RR fashion
// uses getNextChunk helper function
void sendChunkData(char *inputFile, int nMappers) {

  /*open Message Queue*/
  key_t key = ftok("project", 4285922);        //has to be the same key as the one in getChunkdata()
  int msgid = msgget(key, PERM | IPC_CREAT);
  if(msgid < 0) {
    perror("Msg queue open error in sendChunkData");
    return;
  }
  struct msgBuffer msg;
  struct msgBuffer ACKmsg; //used to consider ACK messages
	int send1, send2 = 0;
  ssize_t ACKsent = 0;
	int mapperID = 0;
	long endFile;	//end of file position
	char *chunkArray;

  /*open file*/
  FILE* file = fopen (inputFile, "r");
	fseek(file, 0, SEEK_END); //move pointer to end of file while keeping "file" pointer unaffected
	endFile = ftell(file);	// get the EOF position
	fseek(file, 0, SEEK_SET); //reset to beginning of file

  if(file == NULL) {
		printf("ERROR: Cannot open the file %s\n", wordFileName);
    exit(0);
  }

	while(ftell(file) < endFile) {	//current position < EOF
		//getNextChunk(file, &chunkArray); 	//chunkArray now points to current chunk - getNextChunk void version
    chunkArray = getNextChunk(file); // char* getNextChunk version
		if(chunkArray) { //chunkArray points to a meaningful chunk  --> send
      // Populate msg data structure
      msg.msgType = mapperID+1;
      memcpy(msg.msgText, chunkArray, sizeof(char)*chunkSize);  //copy content of chunkArray into msgText
		  // Send message to queue
			send1 = msgsnd(msgid, (void*) &msg, sizeof(msg.msgText), 0);
      if(send1 < 0){ //send error
        perror("Msg send error in sendChunkData");
        break;
      }
			free(chunkArray);		//will free the bufferChunk in getNextChunk's heap, prevent mem leak
			if(mapperID < nMappers)
				mapperID++;		//move on to next mapperID
			else
				mapperID = 0;		//get back to mapperID 0
		}
	}

	for(int i=0; i<nMappers; i++)	{	//send END messages to mappers
		msg.msgType = i+1;
		memset(msg.msgText, '\0', MSGSIZE);
		sprintf(msg.msgText, "END");
		send2 = msgsnd(msgid, (void*) &msg, sizeof(msg.msgText), 0);  //message of type END
    if(send2 < 0){ //send error
      perror("Msg send error in sendChunkData");
      break;
    }
	}

	/* Wait for ACK from mappers - until have received all messages of type ACK */
  int numberOfACK = 0;
  while(numberOfACK < nMappers){ //not all mappers have sent ACK's - blocking loop
    // receive messages of ACKTYPE sent by getChunkData when mapper receives END msg
    ACKsent = msgrcv(msgid, (void*) ACKmsssize_tg, sizeof(ACKmsg.msgText), ACKTYPE, 0);
    if(ACKsent < 0) { //failed to receive ACK
      perror("ACK failure in sendChunkData");
      continue;
    }
    numberOfACK++;
  }

	/*Close everything*/
	msgctl(msgid, IPC_RMID, NULL); //close msg queue
	fclose(file);

  return;
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
  //Open message queue
  key_t key = ftok("project", 4285922);        //has to be the same key as the one in getChunkdata()
  int msgid = msgget(key, PERM | IPC_CREAT);
  char *mapDir; //Directory path to mapper output, assumes created in the mapping phase
  DIR* currMapDir;
  int pathSend, ENDsend = 0;
  ssize_t ACKsent = 0;
  struct msgBuffer txtPath; //msg contains the path to the word file
  struct msgBuffer endMsg; //end msg to reducers
  char* fileName; // notes the file names for files within directory
  int reducerID; //id of Reducer to be returned by hash
  struct dirent* MapDirEntry;

  if(msgid < 0) {
    perror("Msg queue open error in shuffle");
    return;
  }

  for(int i=0; i<nMappers; i++){
    mapDir = getMapDir(i);
    if((currMapDir = opendir(mapDir)) == -1){
      perror("Directory open error in shuffle");
      break;
    }
    while((MapDirEntry = readdir(currMapDir)) != NULL) {
      if(MapDirEntry->d_type == DT_REG && (fileName = strstr(MapDirEntry->d_name, ".txt")) != NULL){   //Current entry is a regular file of type .txt
        reducerID = hash(MapDirEntry->d_name, nReducers); //Call hash function
        //Populate the fields of txtPath to send to the msg queue
        txtPath.msgType = reducerID;
        //Concatenate text file path into message Text to send to queue
        txtPath.msgText[0] = '\0'; //resets the message to length 0 string
        //memset(endMsg.msgText, '\0', MSGSIZE); //sets a whole string of null characters
        strcpy(txtPath.msgText, mapDir);
        strcat(txtPath.msgText, "/");
        strcat(txtPath.msgText, MapDirEntry->d_name);
        //Send txt file path into queue
        pathSend = msgsnd(msgid, (void*) &txtPath, sizeof(txtPath.msgText), 0);
        if(pathSend < 0) {
          perror("Send error in shuffle");
          break;
        } //pathSend if
      } // txt file entry if
    } //  traversing map output directory loop
    closedir(currMapDir);
  } //   iterate process for all mappers loop

  //Send end msg to reducers
  for(int i=0; i<nReducers; i++)	{
    endMsg.msgType = i+1;
    memset(endMsg.msgText, '\0', MSGSIZE);
    sprintf(endMsg.msgText, "END");
    ENDsend = msgsnd(msgid, (void*) &msg, sizeof(msg.msgText), 0);  //message of type END
    if(ENDsend < 0){ //send error
      perror("Send error in shuffle");
      break;
    }
  }

  int numberOfACK = 0;
  while(numberOfACK < nReducers){ //not all mappers have sent ACK's - blocking loop
    // receive messages of ACKTYPE sent by getChunkData when mapper receives END msg
    ACKsent = msgrcv(msgid, (void*) ACKmsssize_tg, sizeof(ACKmsg.msgText), ACKTYPE, 0);
    if(ACKsent < 0) { //failed to receive ACK
      perror("ACK failure in shuffle");
      continue;
    }
    numberOfACK++;
  }
  // Close everything 
  msgctl(msgid, IPC_RMID, NULL); //close msg queue
  return;
}


int getInterData(char *key, int reducerID) {
}


// Do open and close outside this function so as not to repeatedly open/close the file -> better performance
//	Assume inputFile is opened, write to bufferChunk
/*  For the void function version: char* getNextChunk(FILE* inputFile, char **cArray)  */
//  I think malloc() combats the issue of "cannot point to function's stack after returning" so this might not be necessary
// To access bufferChunk, use a pointer to it, i.e.
// [...] char *array;
// 	getNextChunk(inFile, &array);	//array should now point to bufferChunk, allocated on the heap of this function
//	if(array) //pointer --x--> NULL
//		[...do something...]
// free(array);

/* For the char* return, returns pointer to bufferChunk in memory */
// char* getNextChunk(FILE* inputFile)
// Still, return after calls

char *getNextChunk(FILE* inputFile) {
	char *bufferChunk = (char *)malloc(sizeof(char)*chunkSize); //initialize a buffer for chunk data - chunk size = 1024 bytes
	int currentChar; //used to detect
	if(bufferChunk = NULL) {
		printf("getNextChunk malloc() failure \n");
		exit(0);
	}

	memset(bufferChunk, '\0', chunkSize);
	for(int i=0; i<chunkSize; i++) {		//iteratively call fgetc to write into buffer, all 1024 bytes - consider 1 char = 1 byte
		currentChar = fgetc(inputFile);
		if(currentChar != EOF) { 	//if currentChar is not end-of-file , add it to the bufferChunk array
			bufferChunk[i] = (char)currentChar;
		}
		else {	//has reached EOF
			break; //break out of loop, no point in writing further
		}
	}
	// At this point, all 1024 characters have been filled for bufferChunk
	// If the last character in bufferChunk is not ' ', '/0', we are in the middle of a word
	// gently ungetc backward until you reach a ' ' or '/0' character
	int j = chunkSize - 1; // Starting from last character in chunk
	while(bufferChunk[j] != ' ' && bufferChunk[j] != '\0') {
		ungetc((int)bufferChunk[j], inputFile);	//push the characters in the reverse order, restoring the mangled word tail -> head
    bufferChunk[j] = '\0'; // Pad a '\0' back into ungetc'd char
		j--;
	}
	//*cArray = bufferChunk;	//no return statement, keep buffer allocated on this function's heap --> must free it after each call
	//"returns" a character buffer of full words only (extra slots are padded with '\0')
	return bufferChunk;
}

char *getMapDir(int mapperID){  //just createMapDir without the create part since map outputs have all been created
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	return dirName;
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
