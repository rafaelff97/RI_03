# Authors

Rafael da Fonseca Fernandes nºmec:95319

Gonçalo Junqueira nºmec:95314

# Functioning
run.py is an executable program that will ask the user which action the user will want from indexing, to search and finally analyze while giving the option to exit.
All three options will ask a series of questions for the tokenizer :
- Minimum len of term, anything bellow will be deleted
- Stopwords (NLTK or user defined) If user defined he will be asked to write the words
- PorterStemmer, allow the user to pick between Snowball or NLTK
- All the previous questions do have an option to be skipped if desired
With these questions answered for the program to know how to perform the tokenizer, it will then run the first option that the user chose.
During indexing there is a variable called "finalFileNames" in the program run.py that holds how the merging files will be devided. To get this devision the user must run an analyze, copy the results and paste them in the variable.
If the option chosen is "search" the user will be asked the program will ask what the query is and which method the user prefer to go with (bm25, lnc-ltc), if the choise was bm25 it will later ask the values of k1 and b. It also has a boosting method that it will ask the user if he wishes to apply it and lastly it will ask how many relevant documents the user wishes to see, for example if the user picks 10 then it will print the 10 most relevant documents. At the end it will print the result in the form of term:idf;doc1:score;doc2:score and save the result in a txt file having the query as it's name.

The following files must be created beforehand to ensure proper run next to where the program is placed:
- queryResults
- indexIncompleto
- indexCompletoWeight
- indexCompletoNormalized
- importantValues
- IDFIndex
- documentLength
- documentIndex


# Data Structures:
The program is devided in to three parts, the "run.py", "query.py" and finally the "document.py" the last two being classes.
Run - Can be seen as the main of the program, calls and creates all the necessary classes and runs the methods needed to ensure the program does as it was asked.
Document - This class will be responsible for indexing, merging and analyzing the file and will create the various files required for many of the funcions and calculations done throughout the program.
Query - After merging as been completed we can start searches since it needs certain files that are created during the indexing. This class holds all the necessary methods to ensure the a search is done such as calculating the bm25 values to lnc-ltc and many more.

# Statistics
All stats gathered where for when running on default parameters.