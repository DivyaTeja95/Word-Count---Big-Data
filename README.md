# Word-Count---Big-Data
GOAL: Upload books to HDFS and decompress them in HDFS File System and delete the compressed ones, and run Word Count on all books taken together using Map Reduce

* part1 folder includes the code to upload the files to HDFS File System, decompress them inside HDFS and delete the compressed ones.
* WordCount file includes the rest of the Word Count code.
Steps taken:
- Stop Words are removed in the map phase. A list of stop words has been downloaded from https://www.textfixer.com/tutorials/common- english-words-with-contractions.txt
- All the words less than 5 characters in length, and also special characters are also removed, and all the words are converted into lower case.
