from collections import Counter
from os import listdir
from os.path import isfile, join
import math
import pickle
import sys
import csv
import gzip
import re
import collections
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import PorterStemmer
import time

class Query:
    def __init__(self,nameFileTsvGz, limitPostings,currentPostings,arrayOpc, sizeLimitWord,listStopWords):
        self.currentPostings = currentPostings
        self.limitPostings = limitPostings
        self.sizeLimitWord = sizeLimitWord
        self.arrayOpc = arrayOpc
        self.listStopWords = listStopWords
        self.nameFileTsvGz = nameFileTsvGz
        self.pathIndexCompletoNormalized = "indexCompleteNormalized"
        self.pathIdfIndexDictionary = "IDFIndex/dictionary.txt"
        self.pathImportantValues = "importantValues/values.txt"
        self.pathDocumentIndex = "documentIndex/documentIndex.txt"
        self.pathQueryResults = "queryResults/queryResults.txt"
        self.pathQueriesRelevante = 'queries.relevance.txt'
        
    def noBoost(self,dicDocuments):
        for doc in dicDocuments:
            dicDocuments[doc] = 0
        return dicDocuments
    def calculatePrecision(self,retrieved_docs, relevantList):
        interception = list(set(retrieved_docs) & set(relevantList))
        precision = len(interception) / len(retrieved_docs)
        return precision
    
    def calculateRecall(self,retrieved_docs, relevantList):
        interception = list(set(retrieved_docs) & set(relevantList))
        recall = len(interception) / len(relevantList)
        return recall
    
    
    def calculateF_Measure(self,precision, recall):
        if precision + recall == 0:  # Crash prevention
            return 0.0
        return (2 * precision * recall) / (precision + recall)
    
    
    def calculateAveragePrecision(self,retrieved_docs, relevantList):
        averagePrecision = 0
        ret_docs = []
        relevantCount = 0
        for doc in retrieved_docs:
            ret_docs.append(doc)
            if doc in relevantList:
                averagePrecision += self.calculatePrecision(ret_docs, relevantList)
                relevantCount += 1
    
        if relevantCount == 0:
            return 0
        averagePrecision = averagePrecision / relevantCount
        return averagePrecision
    
    def calculateQueryThroughput(self,time):
        return 1/time

    def getRelevance(self,query):
        dicDocuments = {}
        found = False
        file1 = open('queries.relevance.txt', 'r')
         
        while True:     
            # Get next line from file
            line = file1.readline()
         
            # if line is empty
            # end of file is reached
            if not line:
                break
            if line == "\n":
                continue
            if line[0] == "Q" and line[1] == ":":
                currentQuery = line.split(":")[1].replace("\n","")
                if found:
                    break
                if currentQuery == query:
                    found = True
                    continue
            else:
                if found:
                    data = line.split()
                    dicDocuments[data[0]] = data[1]
                    
        file1.close()
        return dicDocuments
        
    def DCG(self,ranking):
        dcg = 0
        for i, r in enumerate(ranking):
            r = int(r)
            if i < 1:
                dcg = r
            else:
                log = math.log(i, 2)
                if log != 0:     
                    dcg += r / log
        return dcg

    def calculateNCDG(self,query, documentScoreDict, topx):
        dicRelevance = self.getRelevance(query)
        
        ranking = []
        keys = list(documentScoreDict.keys())
        for i in range(topx):
            if keys[i] in dicRelevance:
                ranking.append(dicRelevance[keys[i]])
            else:
                ranking.append(0)
        
        actual_dcg = self.DCG(ranking)
        
        ranking = []
        keys = list(dicRelevance.keys())
        for i in range(topx):
            if keys[i] in dicRelevance:
                ranking.append(dicRelevance[keys[i]])
            else:
                ranking.append(0)
        expected_dcg = self.DCG(ranking)
        
        return actual_dcg / expected_dcg
        
    def getRelevantList(self,query):
        listDocuments = []
        found = False
        file1 = open(self.pathQueriesRelevante, 'r')
         
        while True:     
            # Get next line from file
            line = file1.readline()
         
            # if line is empty
            # end of file is reached
            if not line:
                break
            if line == "\n":
                continue
            if line[0] == "Q" and line[1] == ":":
                currentQuery = line.split(":")[1].replace("\n","")
                if found:
                    break
                if currentQuery == query:
                    found = True
                    continue
            else:
                if found:
                    print(line)
                    listDocuments.append(line.split()[0])         
         
        file1.close()
        return listDocuments

    def analysesComplete(self,table):
        """ Split the table into ranges of x postings, the result is to be used later in the merging """
        postings = self.limitPostings
        splitResults = []
        firstTerm = ""
    
        for key, value in table.items():
            while value != 0:
                if firstTerm == "":
                    firstTerm = key
    
                postings -= value
                value = 0
    
                if postings <= 0:
                    splitResults.append(str(firstTerm + "-" + key))
                    firstTerm = ""
                    postings = self.limitPostings
    
        if postings > 0:
            splitResults.append(str(firstTerm + "-" + key))
    
        print(splitResults)

    def analyses(self):
        """ Read the file and fill in the table with the terms:counter and then do the analysis to know how best to separate by file """
        maxInt = sys.maxsize
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
                    table = self.fillTableAnalysis(tokenizerText, table)
    
        # for key in tabela:
            #print(str(key[0]) + " - " + str(tabela[key[1]]))
        # print(collections.OrderedDict(sorted(tabela.items())))
        self.analysesComplete(collections.OrderedDict(sorted(table.items())))

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
        #print("Lista de stop words:"+str(stops))
        for line in array:
            for w in line.split():
                if w.lower() not in stops:
                    arrayTemp.append(w)
        return arrayTemp
    
    def userDefined(self,array, listt):
        """user defined StopWords"""
        arrayTemp = []
        stopWordsList = listt.split()
        for line in array:
            for w in line.split():
                if w.lower() not in stopWordsList:
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
    def fillTableAnalysis(self, tokenizedText, table):
        """ Update a dictionary of terms:counter with tokenizedText """
        uniqueTerms = set(tokenizedText)
    
        for term in uniqueTerms:
    
            if term in table:
                table[term] += 1
            else:
                table[term] = 1
    
        return table
    
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

    def termWeight(self,counter):
        """ Calculates the weights of the terms returning a dictionary with this information and the query size """
        queryDictionaryWeight = {}
        with open(self.pathIdfIndexDictionary, "rb") as fp:   # Unpickling
            idfTermDictionary = pickle.load(fp)
    
        for term, count in counter.most_common():
            queryDictionaryWeight[term] = (
                1 + math.log10(count)) * idfTermDictionary[term]
    
        queryLength = sum([math.pow(float(v), 2)
                          for k, v in idfTermDictionary.items() if k in counter.keys()])
        return queryDictionaryWeight, queryLength

    def getDocumentIds(self, line):
        arrayDocs = []
        arrayDocInfo = line.split(";",1)[1].split(";")
        
        for docInfo in arrayDocInfo:
            arrayDocs.append(docInfo.split(":")[0])
            
        return arrayDocs
      
    def openAndSearchFile(self, term, file):
        """ Opens a specific file and searches for a specific term returning documents that contain that term """
        file = open( self.pathIndexCompletoNormalized+'/' + file, 'r')
        arrayDocuments = []
        while True:
            # Get next line from file
            line = file.readline()
         
            # if line is empty
            # end of file is reached
            if not line:
                break
            
            termLine = line.split(";")[0]
            
            if termLine == term:
                return self.getDocumentIds(line)
        file.close()
        return arrayDocuments

    def searchDocumentsForTerm(self,arrayTerms):
        """ Returns a set of documents that contain any term in the array """
        indexIncomplete = [f for f in listdir( self.pathIndexCompletoNormalized) if isfile(join(self.pathIndexCompletoNormalized, f))]
        arrayResults = []
        for file in indexIncomplete:
            for term in arrayTerms:
                if self.checkRange(term, file):
                    arrayResults += self.openAndSearchFile(term, file)
    
        return set(arrayResults)
    
    def documentIndexToDocumentId(self, indexedDocuments,limit):
        """ Returns a dictionary where the temporary id of the document is the original """
        resultDict = {}
        topx = 0
        for document in indexedDocuments:
            with open(self.pathDocumentIndex) as f:
                 data = f.readlines()[int(document)]
            f.close()
            documentId = data.split(",")[1]
            resultDict[documentId]= indexedDocuments[document]
            topx += 1
            if topx == limit:
               break
            
        return resultDict
        
    def readTxtFile(self, path):
        f = open(path, "r")
        return f.read()    
    
    def normalizedWeightQuery(self, dictionary):
        total = math.sqrt(sum([math.pow(float(v), 2)
                          for v in dictionary.values()]))
    
        for term in dictionary:
            dictionary[term] = dictionary[term] / total
    
        return dictionary
        
    def loadDocTermPosition(self, indexedDocuments):
        indexCompleto = [f for f in listdir(self.pathIndexCompletoNormalized) if isfile(join(self.pathIndexCompletoNormalized, f))]
        tabela = {}
        for file in indexCompleto:
            
            file1 = open((self.pathIndexCompletoNormalized + "/" + file), 'r', encoding='utf-8')
            
            while True:     
                line = file1.readline()
             
                # if line is empty
                # end of file is reached
                if not line:
                    break
                arrayDocInfo = line.split(";",1)[1].split(";")
                term = line.split(";",1)[0]
                
                for docInfo in arrayDocInfo:
                    docId = docInfo.split(":")[0]                            
                    if docId in indexedDocuments:
                        
                        tempArray = docInfo.split("[")[1].replace("]","").replace(")","").replace(" ","").split(",")
                        arrayPositions = []
                        for position in tempArray:
                            arrayPositions.append(int(position))
                        
                        if docId in tabela:  
                           tabela[docId].update({term: arrayPositions})
                        else:
                           tabela[docId] = {term: arrayPositions}
                                    
            file1.close()
        return tabela
    
    def prepareOrderedDocument(self,dicDocuments, contador, tempArray):
        for term in dicDocuments:
            for value in dicDocuments[term]:
                if contador == value:
                    tempArray.append((term, value))
                    contador += 1
                    return tempArray, True, contador
        return tempArray, False, 0
    
    def splitArray(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        listOfLists = []
        for i in range(0, len(lst), n):
            listOfLists.append(lst[i:i + n])
        return listOfLists
    
    def countTermsInList(self,listTerms, query):
        counterDic = {}
        for term in query:
            counter = 0
            for values in listTerms[1]:
                if values[0] == term:
                    counter += 1
            counterDic[term] = counter
        return counterDic
    
    def boost(self, dicDocuments, query, size):
        #2*exp(x)/(exp(x)+10) quantas vezes o termo aparece na query
        #query com o organzação certa: quando x >= 2 = x*2
        arrayTupleOrganized = []
        dicBoost = {}
        
        for docId in dicDocuments:
            dicBoost[docId] = 0
        
        for doc in dicDocuments:
            restart = True
            tempArray = []
            contador = 0
            while restart:
                tempArray, restart, contador =  self.prepareOrderedDocument(dicDocuments[doc], contador, tempArray)
                if not restart:
                    arrayTupleOrganized.append((doc, tempArray.copy()))
        
        
        
        for doc in arrayTupleOrganized:
            counter = self.countTermsInList(doc, query)
            slippedArrays = self.splitArray(doc[1], size)
            contador = 0
            for arrayTupleInfo in slippedArrays:
                i = 0
                e = 0
                while i < len(arrayTupleInfo):
                    e = 0
                    contador = 0
                    while e < len(query):
                        if (arrayTupleInfo[i][0] == query[e]):
                          contador = 1
                          while(i+1 < len(arrayTupleInfo) and e+1 < len(query) and arrayTupleInfo[i+1][0] == query[e+1]):             
                                contador += 1
                                i += 1
                                e += 1
                          if contador > 1:
                              dicBoost[doc[0]] += contador * 2
                        e += 1
                    i += 1
            for term in query:
                try:
                    ans = math.exp(counter[term])
                except OverflowError:
                    ans = 0
                dicBoost[doc[0]] += 2*ans /(ans + 10)
            
        return dicBoost
    
    def loadTermDocumentNormalized(self, indexedDocuments):
        """ Returns a dictionary with the format: docId: {term:peso} """
        #term, documentId, nVezesQueOTermoApareceNoDocumento, weight
        indexComplete = [f for f in listdir(self.pathIndexCompletoNormalized) if isfile(
            join(self.pathIndexCompletoNormalized, f))]
        table = {}
        for file in indexComplete:
    
            file1 = open((self.pathIndexCompletoNormalized+"/" + file),
                         'r', encoding='utf-8')
    
            while True:
                line = file1.readline()
    
                if not line:
                    break
                arrayDocInfo = line.split(";",1)[1].split(";")
                term = line.split(";",1)[0]
                
                for docInfo in arrayDocInfo:
                    docId = docInfo.split(":")[0]                            
                    if docId in indexedDocuments:
                       weight = float(docInfo.split(":")[1].replace("(","").replace(")","").split(",")[1])    
                       if docId in table:  
                         table[docId].update({term: weight})
                       else:
                        table[docId] = {term: weight}
            file1.close()
        return table
     
    def score(self,documentsNWeight, queryNWeight, boostTable):
        """ Calculates the score and returns a dictionary with the format: term: {docid:score} """
        dictResults = {}
    
        for document in documentsNWeight:
            for term in documentsNWeight[document]:
                if term in queryNWeight.keys():
                    score = float(documentsNWeight[document][term]) * queryNWeight[term]
                    score += boostTable[document]
                    if document in dictResults.keys():
                        dictResults[document] += score
                    else:
                        dictResults[document] = score
        dictResults = dict(sorted(dictResults.items(), key = lambda x: x[1], reverse = True))
        return dictResults
        
    def countDocumentsForTerm(self,term, file):
        """ Counts the number of times a term in a specific file """
        file = open( self.pathIndexCompletoNormalized+'/' + file, 'r')
        found = False
        counter = 0
        while True:
            # Get next line from file
            line = file.readline()
         
            # if line is empty
            # end of file is reached
            if not line:
                break
            termDocument = line.split(";",1)[0]
            
            if termDocument == term:
                arrayDocInfo = len(line.split(";",1)[1].split(";"))
                found = True
                counter = arrayDocInfo
                file.close()
                return counter
            elif found:
                break 
        file.close()
        return counter
        
    def tfi(self,term):
        """ Counts the number of documents that contain a specific term """
        indexIncompleto = [f for f in listdir( self.pathIndexCompletoNormalized) if isfile(
            join( self.pathIndexCompletoNormalized, f))]
        totalDocumentsForTerm = 0
        for file in indexIncompleto:
            if self.checkRange(term, file):
                totalDocumentsForTerm = self.countDocumentsForTerm(term, file)
    
        return totalDocumentsForTerm

    def bm25(self, k1, b, query, documents, boostTable):
        """ Does the maths for bm25 """
        dictDocumentBM25 = {}
        dictTFI = {}
        for term in query:
            dictTFI[term] = self.tfi(term)
    
        documentTermDictionary = self.loadTermDocumentNormalized(documents)
    
        with open(self.pathIdfIndexDictionary, "rb") as fp:   # Unpickling
            idfTermDictionary = pickle.load(fp)
    
        f = open(self.pathImportantValues, "r")
        mediaLenDocuments = float(f.readline().split(",")[0])
        f.close()
    
        f = open(self.pathDocumentIndex, "r")
        content = f.readlines()
        for documentIndex in documents:
            bm25Document = 0
            lenDocument = content[int(documentIndex)].split(",")[2]
             
            for term in query:
                if term in documentTermDictionary[documentIndex].keys():         
                    mediaDocument = float(lenDocument)/mediaLenDocuments
                    tfTerm = dictTFI[term]
                    bm25Document += idfTermDictionary[term] * (((k1 + 1) * tfTerm)) / (k1 * ((1 - b) + b * mediaDocument) + tfTerm)
        
            dictDocumentBM25[documentIndex] = bm25Document + boostTable[documentIndex]
                         
        dictDocumentBM25 = dict(sorted(dictDocumentBM25.items(), key = lambda x: x[1], reverse = True))
        f.close()
        return dictDocumentBM25
        
    def printResultQuery(self, documentIndex):
        """ Prints the query results in a more organised manner """
       # with open(self.pathIdfIndexDictionary, "rb") as fp: 
       #     idfTermDictionary = pickle.load(fp)
            
        with open(self.pathQueryResults, 'w') as f:
    
            termInfo = ""
            for document in documentIndex:
               termInfo += str(document) + " - " + str(round(documentIndex[document],2)) + "\n"
            #print(termInfo)
            f.write(termInfo)
                #f.write("\n")
            termInfo = ""
        f.close()
    
    def query(self):
        """ Method in charge of doing all the calculations and showing the query results """
        
        queryOriginal = str(input("What is your query:\n"))
        
        requiredBoost = str(input("Do you wish to apply boostings?:\n" +
                               "1 -> Yes\n" + 
                               "2 -> No\n"))

        method = str(input("Pick your prefered method to run:\n" +
                       "1 -> lnc-ltc\n" +
                       "2 -> bm25\n"))        
        
        topx = int(input("The number of documents you wish to see as a result (top 10, top15, topx):\n"))
        start_time = time.time()
        relevantDocuments = self.getRelevantList(queryOriginal)

        query  = self.tokenizer(queryOriginal)
        counts = Counter(query)
        dictionaryTermWeight, queryLength = self.termWeight(counts)  
        indexedDocuments = self.searchDocumentsForTerm(query)
        
        boostTable = self.loadDocTermPosition(indexedDocuments)
        
        if requiredBoost == "1":
            boostTable = self.boost(boostTable, query, 5)
        elif requiredBoost == "2":
            boostTable = self.noBoost(boostTable)
        
        
        if method == "1":
            dictionaryTermNormalizedWeight = self.normalizedWeightQuery(
                dictionaryTermWeight)
            dictDocumentsNormalized = self.loadTermDocumentNormalized(indexedDocuments)
            finalDict = self.score(dictDocumentsNormalized,
                              dictionaryTermNormalizedWeight, boostTable)
        elif method == "2":
            k1 = float(input("What is the value of k1:\n"))
            b = float(input("What is the value of b:\n"))
            if k1<=0 or b<=0:
                k1=1.2
                b=0.75
            finalDict = self.bm25(k1, b, query, indexedDocuments, boostTable)

        documentScoreDict = self.documentIndexToDocumentId(finalDict, topx)
        timeForquery = time.time() - start_time
        self.printResultQuery(documentScoreDict)
        
        if len(relevantDocuments) != 0:     
            prec = self.calculatePrecision(documentScoreDict, relevantDocuments)
            recall = self.calculateRecall(documentScoreDict, relevantDocuments)
            fMeasure = self.calculateF_Measure(prec, recall)
            averagePrec = self.calculateAveragePrecision(documentScoreDict, relevantDocuments)
            queryThroughput = self.calculateQueryThroughput(timeForquery)
            queryLatency = timeForquery
            NDCG = self.calculateNCDG(queryOriginal, documentScoreDict, topx)
            
            print("-------")
            print("Prec: ",prec)
            print("Recall: ",recall)
            print("fMeasure: ",fMeasure)
            print("Average Prec:",averagePrec)
            print("Query Throughput:" ,queryThroughput)
            print("Query Latency:",queryLatency)
            print("NDCG:",NDCG)
        