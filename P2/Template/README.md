README

**NOTE**
THIS SUBMISSSION SUPERCEEDS THE PREVIOUS SUBMISSION FROM MINH PRIOR TO THE EXTENDED DEADLINE

CSCI4061 Fall 2020
Project 1 - Group 41
Group members:
    Minh Bui / bui00011
    John Kimlinger / kimli020
    Andrew Trudeau / trude135

• The purpose of your program:
  The goal of the project is to implement a simple mapreduce operation on a text file. The mapreduce operation outputs information on how often each word occurs in the original file. 
  In order to accomplish this task, some functions use Inter-Process Communication in order to send data from the file to different processes.

• How to compile and run the program
  1. Naviagate to the Template folder of the project, using something like: $ cd [project_folder_path]/Template
  2. Run the make command. $make
  3. Then the command $./mapreduce #mappers #reducers inputFile. Example: Example: $./mapreduce 5 2 test/T1/F1.txt
        Where #mappers = number of mapper processes 
        #reducers is the number of reducer processes, 
        and inputFile is pathname to the text file you wish to run MapReduce on. 

• What exactly your program does
     Unfortunately, our project does work as intended. Many hours of troubleshooting were spent debugging this program, but without success. It does compile, and attempts to run, but hangs without ever finishing. 

• Any assumptions outside this document
    

• Contribution by each member of the team
  Minh Bui:  Shuffle(),troubleshooting and debugging
  John Kimlinger: sendChunkData(), getChunkData(), troubleshooting and debugging
  Andrew Trudeau: getInterData(), augmenting utils.h, troubleshooting and debugging

