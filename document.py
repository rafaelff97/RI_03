from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import PorterStemmer
import gzip
import csv
import sys
import re
import pickle
from os import listdir
from os.path import isfile, join
from collections import Counter
import math
import os

class Document:
    def __init__(self, nameFileTsvGz, limitPostings,currentPostings,arrayOpc, sizeLimitWord,listStopWords, finalFileNames):
        self.currentPostings = currentPostings
        self.limitPostings = limitPostings
        self.sizeLimitWord = sizeLimitWord
        self.arrayOpc = arrayOpc
        self.finalFileNames = finalFileNames
        self.listStopWords = listStopWords
        self.nameFileTsvGz = nameFileTsvGz
        self.pathIndexCompleteWeight = "indexCompleteWeight"
        self.pathIndexIncomplete =  "indexIncomplete"
        self.pathImportantValues = "importantValues/values.txt"
        self.pathDocumentLength = "documentLength/documentLength.txt"
        self.pathIndexCompleteNormalized = "indexCompleteNormalized"
        self.pathDictionary = "IDFIndex/dictionary.txt"
    
    def getPositions(self,tokenizedText):
        dictPositions = {}
        
        for i in range(0, len(tokenizedText)):
            if tokenizedText[i] in dictPositions:  
                dictPositions[tokenizedText[i]].append(i)
            else:
                dictPositions[tokenizedText[i]] = [i]
                
        return dictPositions

    def fillTable(self,idDocument, tokenizedText, table):
        """ Fills in the dictionary with relevant information and returns the updated dictionary """
        #term: docID1: (nVezesQueAparece, arrayPosicoes); docID2: (nVezesQueAparece, arrayPosicoes)
        uniqueTerms = set(tokenizedText)
        counter = Counter(tokenizedText)
        dictPositions = self.getPositions(tokenizedText)
        
        for term in uniqueTerms:
            if term in table:
                extraInfoTuple = (counter[term], dictPositions[term])
                table[term].update({idDocument: extraInfoTuple})
            else:
                extraInfoTuple = (counter[term], dictPositions[term])
                table[term] = {idDocument: extraInfoTuple}
    
            self.currentPostings += 1

        return table
    
    def writeTextFile(self,path, text):
        f = open(path, "w", encoding="utf-8")
        f.write(text)
        f.close()
      
    def writeList(self,listDocuments):
        with open("documentIndex/tabela.txt", "wb") as fp:   #Pickling
            pickle.dump(listDocuments, fp)
            
    def loadListDocuments(self):
        with open("documentIndex/tabela.txt", "rb") as fp:   # Unpickling
            arrayDocumentos = pickle.load(fp)
        
        return arrayDocumentos
    
    def writeIndexingDisorganised(self,table):
        """ Creates a string with the format: termo;documentoId:(nVezesQueTermoApareceDocumento,Peso, arrayPosicoes);documentoId:(nVezesQueTermoApareceDocumento,Peso, arrayPosicoes) e guarda num ficheiro txt"""
        sort_keys = table.items()
        new_items = sorted(sort_keys)
        indexFinal = dict(new_items)
        stringFinal = ""
    
        for term in indexFinal:
            stringFinal += term
            for documentId in indexFinal[term]:
               weight = round(1 + math.log10(indexFinal[term][documentId][0]), 2)
               stringFinal += ";" + str(documentId) + ":(" + str(indexFinal[term][documentId][0]) + "," + str(weight) + "," + str(indexFinal[term][documentId][1]) + ")"
            stringFinal  += "\n"
        currentFileNumber = len(
            [f for f in listdir(self.pathIndexIncomplete) if isfile(join(self.pathIndexIncomplete, f))])
        self.writeTextFile(self.pathIndexIncomplete+"/" +
                              str(currentFileNumber) + ".txt", stringFinal)
        
    def firstFromList(listTerms):   
        return sorted(listTerms)[0]
    
    def readSpecificPart(path, start, finish):
        """ Reads a specific chunk from a txt file and returns the data from that chunk """
        with open(path) as fin:
            fin.seek(start)
            data = fin.readLines(finish - start)
        return data

    def readChunk(self,path, start, finish):
        """ Load the txt file and then return a specific chunk of it """
        # open the sample file used
        file = open(path, encoding="utf8")
          
        # read the content of the file opened
        content = file.readlines()
        
        data = content[start:finish]
        
        file.close()
        
        return data

    def addToDict(self,table, row, rangeFile): #Acho que nem uso este metodo
        
        values = row.split(",")
        term = values[0]
        documentId = values[1]
        tf = values[2]
        if not self.checkRange(term, rangeFile):
            return table
        
        if term in table:  
            table[term].update({documentId: tf})
        else:
            table[term] = {documentId: tf}
                
        self.currentPostings += 1
        return table

    def readRow(self, row):
        dictResults = {}
        term = row.split(";",1)[0]
        arrayDocInfo = row.split(";",1)[1].split(";")
        
        for docInfo in arrayDocInfo:
            docId = docInfo.split(":")[0]
            numberofTimesDocumentAppears = int(docInfo.split(":")[1].replace("(","").replace(")","").split(",")[0])
            weight = float(docInfo.split(":")[1].replace("(","").replace(")","").split(",")[1])
            tempArray = docInfo.split("[")[1].replace("]","").replace(")","").replace(" ","").split(",")
            arrayPositions = []
            for position in tempArray:
                #position.replace("/n","")
                arrayPositions.append(int(position))
            if term in dictResults:  
                dictResults[term].update({docId:(numberofTimesDocumentAppears, weight, arrayPositions)})
            else:
                dictResults[term] = {docId:(numberofTimesDocumentAppears, weight, arrayPositions)}
        return dictResults

    def fillFinalTable(self, data, table, rangeFile):
        """ Adds the information to a dictionary and returns the dictionary """
        #termo;documentoId:(nVezesQueTermoApareceDocumento,Peso, arrayPosicoes);documentoId:(nVezesQueTermoApareceDocumento,Peso, arrayPosicoes)
        for rowsDocument in data:
            for row in rowsDocument:
                rowInfo = self.readRow(row.rstrip("\n").rstrip(""))
                term = row.split(";",1)[0]
                if self.checkRange(term, rangeFile):
                    if term in table:  
                        table[term].update(rowInfo[term])
                    else:
                       table[term] = rowInfo[term]
        
        return table

    def readFileSize(self, path):
        """Devolve quantas linhas um ficheiro de texto tem"""
        file = open(path, "r", encoding="utf8")
        line_count = 0
        for line in file:
            if line != "\n":
                line_count += 1
        file.close()
        return line_count

    def loadDataFromFiles(self,start, finish, fileList):
        """Tenta carregar para um dicionario um chunk de informação dum ficheiro de texto, devolve os dados que carregou e um boolean a dizer se carregou alguma coisa ou não"""
        data = []
        didntLoad = True
        for file in fileList:
            path = self.pathIndexIncomplete + "/" + file
            if self.readFileSize(path) >= start:
                didntLoad = False
                data.append(self.readChunk(path, start, finish))
                
        return data, didntLoad

    def loadTermDocument(self):
        """Returns a dictionary with documents as the key and term as the next key giving the weight at the end"""
        #term, documentId, nVezesQueOTermoApareceNoDocumento, weight
        indexComplete = [f for f in listdir(
            self.pathIndexCompleteWeight) if isfile(join(self.pathIndexCompleteWeight, f))]
        table = {}
        for file in indexComplete:
    
            file1 = open((self.pathIndexCompleteWeight+"/" + file), 'r', encoding='utf-8')
    
            while True:
                line = file1.readline()
    
                if not line:
                    break
                arrayDocInfo = line.split(";",1)[1].split(";")
                term = line.split(";",1)[0]
                
                for docInfo in arrayDocInfo:
                    docId = docInfo.split(":")[0]
                    weight = float(docInfo.split(":")[1].replace("(","").replace(")","").split(",")[1])
                        
                    if docId in table:  
                        table[docId].update({term: weight})
                    else:
                        table[docId] = {term: weight}
            file1.close()
        return table

    def sortAndWritteDocumentLengthDictionary(self,documentLengthDictionary):
        """Ordena e escreve num ficheiro de texto os valores do dicionario"""
        documentLengthDictionary = sorted(documentLengthDictionary.items())
        finalString = ""
        for key, value in documentLengthDictionary:
            finalString += str(key) + "," + str(value) + "\n"
    
        self.writeTextFile(self.pathDocumentLength, finalString)     

    def weightNormalized(self):
        """Sorts and writes the dictionary values to a text file"""
        indexComplete = [f for f in listdir(
            self.pathIndexCompleteWeight) if isfile(join(self.pathIndexCompleteWeight, f))]
        documentLengthDictionary = {}
        termDocument = self.loadTermDocument()
        for file in indexComplete:
            table = {}
            file1 = open(self.pathIndexCompleteWeight +"/" + file, 'r', encoding='utf-8')
            
            while True:     
                # Get next line from file
                line = file1.readline()
            
                # if line is empty
                # end of file is reached
                if not line:
                    break
                rowInfo = self.readRow(line.rstrip("\n").rstrip(""))
            
                term = line.split(";",1)[0]
                if term in table:  
                    table[term].update(rowInfo[term])
                else:
                    table[term] = rowInfo[term]
                  
            for key in table:
                for documentId in table[key]:
                    values = termDocument[documentId]
                    combinedWeight = math.sqrt(
                        sum([math.pow(float(v), 2) for v in values.values()]))
                    documentLengthDictionary[documentId] = combinedWeight
                    primaryTerm = [float(v) for k, v in values.items() if k == key]
                    table[key][documentId] =(table[key][documentId][0], (primaryTerm[0] / combinedWeight), table[key][documentId][2])
    
            self.writeFileFinal((self.pathIndexCompleteNormalized+"/" + file), table)
        self.sortAndWritteDocumentLengthDictionary(documentLengthDictionary)     

    def writeFileFinal(self,fileName, table):
        """ Writes to a specific file the information contained in the dictionary """
        #term, documentId, nVezesQueOTermoApareceNoDocumento, weight
        #termo;documentoId:(nVezesQueTermoApareceDocumento,Peso, arrayPosicoes);documentoId:(nVezesQueTermoApareceDocumento,Peso, arrayPosicoes)
        stringFinal = ""
        for term in table:
            stringFinal += term
            for documentId in table[term]:
                stringFinal += ";" + str(documentId) + ":(" + str(table[term][documentId][0]) + "," + str(table[term][documentId][1]) + "," + str(table[term][documentId][2]) + ")"
            stringFinal += "\n"
        self.writeTextFile(str(fileName), stringFinal)

    def calculateIDF(self,idfDictionary, nTotalDocuments, table):
        """ Update the idfDictionary with new terms and table values """
        for key in table:
            idfDictionary[key] = math.log10(nTotalDocuments / len(table[key]))
    
        return idfDictionary
    
    def merger(self,nTotalDocuments):
        """ Start merge according to pre-defined range """
        idfDictionary = {}
    
        indexIncompleto = [f for f in listdir(self.pathIndexIncomplete) if isfile(join(self.pathIndexIncomplete, f))]
    
        for finalFile in self.finalFileNames:
            start = 0
            finish = self.limitPostings
            table = {}
            finishedFile = False
            while not finishedFile:
                data, didntLoad = self.loadDataFromFiles(start, finish, indexIncompleto)
               
                if didntLoad:
                    #tabela = termWeight(tabela, nTotalDocumentos)
                    idfDictionary = self.calculateIDF(idfDictionary, nTotalDocuments, table)
                    self.writeFileFinal(self.pathIndexCompleteWeight+"/" + finalFile, table)
                    finishedFile = True
                    continue
                
                table = self.fillFinalTable(data, table, finalFile)
                start += self.limitPostings
                finish += self.limitPostings           
                    
        print("Total number of terms: "+str(len(idfDictionary)))
        with open(self.pathDictionary, "wb") as fp:
            pickle.dump(idfDictionary, fp)
        
    def fileToDict(self,path):
        """Reads a specific text file and passes the information to a dictionary"""
        #term, documentId, nVezesQueOTermoApareceNoDocumento, weight
        file = open(path, 'r', encoding='utf-8')
        table = {}
    
        while True:
            line = file.readline()
    
            if not line:
                break
            values = line.strip().split(",")
    
            if values[0] in table:
                table[values[0]].update({values[1]: (values[2], values[3])})
            else:
                table[values[0]] = {values[1]: (values[2], values[3])}
        file.close()
        return table

    def termsInDocument(self,documentId):
        """Returns an array of tuples with the term and weight of a given document"""
        #return tuple with term and weight
        indexCompleto = [f for f in listdir(self.pathIndexCompleteWeight) if isfile(join(self.pathIndexCompleteWeight, f))]
        arrayResults = []
        for file in indexCompleto:
            tabela = self.fileToDict((self.pathIndexCompleteWeight + "/" + file))
            
            for key in tabela:
                for termDocument in tabela[key]:
                    if termDocument == documentId:
                        arrayResults.append((key, tabela[key][termDocument][1]))
        
        return arrayResults   
        
    def fillTableAnalysis(tokenizedText, table):
        """ Update a dictionary of terms:counter with tokenized Text """
        uniqueTerms = set(tokenizedText)
        
        for term in uniqueTerms:
    
            if term in table:
                table[term] += 1
            else:
                table[term] = 1
             
        return table    
        
    def writteDocumentIndex(self,documentIndex):
        """ Prepare the string and then write in a txt file the index of the documents: idTemp,idOriginal,SizeDocument """
        finalString = ""
        path = "documentIndex/documentIndex.txt"
        for index in range(0, len(documentIndex)):
            finalString += str(index) + "," + str(
                documentIndex[index][0]) + "," + str(documentIndex[index][1]) + "\n"
    
        self.writeTextFile(path, finalString)        
        
    def index(self):
       """ Reads the file, prepares the temporary files and then runs the merger to sort/organise while doing some relevant calculations """
       maxInt = sys.maxsize
       documentIndexList = []
       while True:
           try:
               csv.field_size_limit(maxInt)
               break
           except OverflowError:
               maxInt = int(maxInt/10)
   
       with gzip.open(self.nameFileTsvGz, 'rt', encoding='utf-8') as f:
           tsv_reader = csv.reader(f, delimiter="\t")
           line_count = 0
           table = {}
   
           for row in tsv_reader:
               if line_count == 0:
                   line_count += 1
               else:
                   try:
                       tokenizerText = self.tokenizer(row[5], row[13], row[12])
                   except:
                     print("One of the rows had an error so it was skipped!")
                   tupleDocument = (row[2], len(tokenizerText))
                   documentIndexList.append(tupleDocument)
                   table = self.fillTable(len(documentIndexList) -
                                      1, tokenizerText, table)
   
               if self.currentPostings >= self.limitPostings:
                   self.writeIndexingDisorganised(table)
                   self.currentPostings = 0
                   table.clear()
   
           nTotalDocumentos = len(documentIndexList)
   
           mediaLenDocumentos = 0
           for document in documentIndexList:
               mediaLenDocumentos += document[1]
           mediaLenDocumentos = mediaLenDocumentos / nTotalDocumentos
   
           mediaAndTotal = str(mediaLenDocumentos) + "," + str(nTotalDocumentos)
           self.writeTextFile(self.pathImportantValues, str(mediaAndTotal))
   
       self.writteDocumentIndex(documentIndexList)
   
       self.currentPostings = 0
       print("Temporary index number: " + str(len([f for f in listdir(self.pathIndexIncomplete) if isfile(join(self.pathIndexIncomplete, f))])-1))
       self.merger(nTotalDocumentos)
       print("File size index:"+ str(os.path.getsize(self.pathIndexCompleteWeight)))     
        
        
    def minimumLength(self,array, size):
        """ Returns an array whose terms are larger than the specified size """
        new_list = []
        for e in array:
            if len(e) > size:
                new_list.append(e)
        return new_list
    
    def defaultListSW(self,array):
        """ Returns an array of all terms that do not belong to the stopwords list """
        stops = set(stopwords.words('english'))
        arrayTemp = []
        for line in array:
            for w in line.split():
                if w.lower() not in stops:
                    arrayTemp.append(w)
        return arrayTemp
    
    def userDefined(self,array, listt):
        """user defined StopWords"""
        arrayTemp = []
        stopWordsLista = listt.split()
        for line in array:
            for w in line.split():
                if w.lower() not in stopWordsLista:
                    arrayTemp.append(w)
        return arrayTemp
    
    def defaultListPS(self,array):
        ps = PorterStemmer()
        arrayTemp = []
        for w in array:
            arrayTemp.append(ps.stem(w))
        return arrayTemp
    
    
    def snowBall(self,array):
        snow_stemmer = SnowballStemmer(language='english')
        stem_words = []
        for w in array:
            x = snow_stemmer.stem(w)
            stem_words.append(x)
    
        return stem_words
    
    def tokenizer(self,*args):
        textoArray = []
        for text in args:
            text = self.cleaner(text)
            textoArray += text.split()
    
        if self.arrayOpc[0] == '1':
            textoArray = self.minimumLength(textoArray, self.sizeLimitWord)
        elif self.arrayOpc[0] == '2':
            textoArray = textoArray
    
        if self.arrayOpc[1] == '1':
            textoArray = textoArray
        elif self.arrayOpc[1] == '2':
            textoArray = self.defaultListSW(textoArray)
        elif self.arrayOpc[1] == '3':
            textoArray = self.userDefined(textoArray, self.listStopWords)
    
        if self.arrayOpc[2] == '1':
            textoArray = textoArray
        elif self.arrayOpc[2] == '2':
            textoArray = self.defaultListPS(textoArray)
        elif self.arrayOpc[2] == '3':
            textoArray = self.snowBall(textoArray)
        return textoArray      
        
    def cleaner(self,text):
        """ Erase symbols """
        emoji_pattern = re.compile("[^\w']|_", flags=re.UNICODE)
        return (emoji_pattern.sub(" ", text))
        
    def checkRange(self,term, rangeFile):
        """ Checks if a term belongs to a file """
        splittedRange = rangeFile.split("-")
        splittedRange.append(term)
        organizedTerms = sorted(splittedRange)
    
        if term == organizedTerms[1]:
            return True
        else:
            return False    
