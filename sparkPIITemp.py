#Parallel Implementation of PII algorithm with spark
#Assuming n parallel processors

#rowMatchingLookUp<row, column> columnMatchingLookUp<column, row>
 #[preferenceLeft][preferenceRight][rowIndex][rowEnd][columnEnd][ticker]**

import random
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("PII").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

from framework import *

#import pyspark

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.master("local[1]").appName("PII").getOrCreate()

class Generator:

    def generatePreference(n):
        #Takes an integer value n and generates
        #2n random preference lists (n men, n women)
        #with preferenceStructure[i][j] = i^th person's selection at ranking j
        #[preferenceLeft][preferenceRight][rowIndex][rowEnd][columnEnd][ticker]**

        preferenceStructure = [[[int for kk in range(6)] for jj in range(n)] for ii in range(n)]
        nums = list(range(1, n + 1))
        #generate man prefgerences and initialize data structure
        for ii in range(0, n):

            randomNumbers = nums[:]
            random.shuffle(randomNumbers)

            for jj in range(0, n):

                preferenceStructure[ii][jj][0] = randomNumbers[jj]
                preferenceStructure[ii][jj][2] = ii
                preferenceStructure[ii][jj][3] = -1
                preferenceStructure[ii][jj][4] = -1
                preferenceStructure[ii][jj][5] = -1
        #generate women preferences
        for jj in range(0, n):

            randomNumbers = nums[:]
            random.shuffle(randomNumbers)

            for ii in range(0, n):

                preferenceStructure[ii][jj][1] = randomNumbers[ii]

        return preferenceStructure

    def showPreference(preferenceStructure, n):
        #prints out preference structure and salient information
        for ii in range(0, n):
            for jj in range(0, n):
                print(preferenceStructure[ii][jj][0], preferenceStructure[ii][jj][1], end = "  ")
                # preferenceStructure[ii][jj][2], preferenceStructure[ii][jj][3], 
                # preferenceStructure[ii][jj][4], end = "  ")
            print("")

    def resetPreference(preferenceStructure, n):
        #sets values 
        for ii in range(0, n):
            for jj in range(0, n):
                preferenceStructure[ii][jj][2] = preferenceStructure[ii][jj][3] = preferenceStructure[ii][jj][4] =  preferenceStructure[ii][jj][5] = -1

class InitializationMethods:

    def nearStableMatching(preferenceStructure, n):
        #Initializiation function. Sets each man to highest available women on
        #preference List
        rowMatchingLookUp = {}
        columnMatchingLookUp = {}

        for ii in range(0, n):

            matched = False
            currPreference = 1
            jj = 0

            while(not matched):

                if preferenceStructure[ii][jj][0] == currPreference:

                    if columnMatchingLookUp.get(jj) == None:
                        matched = True
                        columnMatchingLookUp[jj] = [ii, preferenceStructure[ii][jj][0], preferenceStructure[ii][jj][1]]
                        rowMatchingLookUp[ii] = jj

                    else:
                        currPreference += 1
                
                jj += 1

                if jj >= n:
                    jj = 0

        return [rowMatchingLookUp, columnMatchingLookUp]

    def randomMatching(preferenceStructure, n):
        # Initializaiton function. Randomly generates a matching.
        nums = list(range(0, n))
        random.shuffle(nums)
        rowMatchingLookUp = {}
        columnMatchingLookUp = {}

        for jj in range(0, n):
            columnMatchingLookUp[jj] = [nums[jj], preferenceStructure[nums[jj]][jj][0], preferenceStructure[nums[jj]][jj][1]]
            rowMatchingLookUp[nums[jj]] = jj

        return [rowMatchingLookUp, columnMatchingLookUp]

class PII:

    def findNMI(preferenceRDD, currColumnMatching, preferenceStructure):
        #finds nm1 pairs and returns as list
        def finder(row, currColumnMatching):
            #worker node function. For each row, 
            #identifies the most minimal unstable pair
            rowIndex = row[0][2]
            columnIndex = -1
            #find the LEFT value of the current pair in the matching
            for key in currColumnMatching:
                if currColumnMatching[key][0] == rowIndex:
                    currLeftValue = currColumnMatching[key][1]
                    break
            #for each entry in the row, check if its LEFT value is smaller than the current matching
            for jj in range(0, len(row)):

                if (row[jj][0] < currLeftValue):
                    #if it is, check to see if its RIGHT value is smaller than the current matching in that column
                    if (row[jj][1] < currColumnMatching[jj][2]):
                        #if it is, the pair is unstable and we update
                        currLeftValue = row[jj][0]
                        columnIndex = jj

            return [rowIndex, columnIndex]
        
        unstableRowPairs = []
        unstablePairs = []
        unstablePairsDict = {}
        #find minimum unstable pir in each row
        unstableRowPairs = preferenceRDD.map(lambda row : finder(row, currColumnMatching)).collect()
        #select unstable pair with lower RIGHT value if both in same column
        for ii in range(0, len(unstableRowPairs)):

             currColumnIndex = unstableRowPairs[ii][1]

             if currColumnIndex > -1:

                currRowIndex = unstableRowPairs[ii][0]
                currRightValue = preferenceStructure[currRowIndex][currColumnIndex][1]

                if unstablePairsDict.get(currColumnIndex) == None:

                    unstablePairsDict[currColumnIndex] = [currRowIndex, currRightValue]

                elif  currRightValue < unstablePairsDict[currColumnIndex][1]:

                    unstablePairsDict[currColumnIndex] = [currRowIndex, currRightValue]
        
        for key in unstablePairsDict:
            unstablePairs.append([unstablePairsDict[key][0], key])

        return unstablePairs

    def findNM2(rowMatchingLookUp, columnMatchingLookUp, preferenceStructure, nm1, ITER):
        #finds nm2 pairs and returns as list of indices
        nm2GeneratingIndices = []
        rowEnds = []
        columnEnds = []
        nm2Pairs = []

        for pair in nm1:
            #get nm1 row,column indices
            nm1RowIndex = pair[0]
            nm1ColumnIndex = pair[1]
            #get indices of old matching pairs in row and column
            rowNM2GeneratingIndices = [nm1RowIndex, rowMatchingLookUp[nm1RowIndex]]
            columnNM2GeneratingIndices = [columnMatchingLookUp[nm1ColumnIndex][0], nm1ColumnIndex]
            #udpate values in matrix
            if preferenceStructure[rowNM2GeneratingIndices[0]][rowNM2GeneratingIndices[1]][5] != ITER:

                preferenceStructure[rowNM2GeneratingIndices[0]][rowNM2GeneratingIndices[1]][5] = ITER
                preferenceStructure[rowNM2GeneratingIndices[0]][rowNM2GeneratingIndices[1]][3] = 1
                preferenceStructure[rowNM2GeneratingIndices[0]][rowNM2GeneratingIndices[1]][4] = 0
            
            else:

                preferenceStructure[rowNM2GeneratingIndices[0]][rowNM2GeneratingIndices[1]][3] = 1

            if preferenceStructure[columnNM2GeneratingIndices[0]][columnNM2GeneratingIndices[1]][5] != ITER:

                preferenceStructure[columnNM2GeneratingIndices[0]][columnNM2GeneratingIndices[1]][5] = ITER
                preferenceStructure[columnNM2GeneratingIndices[0]][columnNM2GeneratingIndices[1]][3] = 0
                preferenceStructure[columnNM2GeneratingIndices[0]][columnNM2GeneratingIndices[1]][4] = 1
            
            else:

                preferenceStructure[columnNM2GeneratingIndices[0]][columnNM2GeneratingIndices[1]][4] = 1

            nm2GeneratingIndices.append(rowNM2GeneratingIndices)
            nm2GeneratingIndices.append(columnNM2GeneratingIndices)
        
        for ii in range(0, len(nm2GeneratingIndices)):

            currRowIndex = nm2GeneratingIndices[ii][0]
            currColumnIndex = nm2GeneratingIndices[ii][1]
            rowEnd = preferenceStructure[currRowIndex][currColumnIndex][3]
            colEnd = preferenceStructure[currRowIndex][currColumnIndex][4]
            #the nm2 generating pair is just a row end
            if (rowEnd == 1) and (colEnd == 0):
                    rowEnds.append(currColumnIndex)
            #the nm2 generating pair is just a column end
            elif (rowEnd == 0) and (colEnd == 1):
                    columnEnds.append(currRowIndex)
        rowEnds.sort()
        columnEnds.sort()
        a = rowEnds.len()

        rowMatchingLookUp = []
        columnMatchingLookUp = []

        for ii in range(0, a):
            matched = False
            currPreference = 1
            jj = 0

            while(not matched):

                if preferenceStructure[columnEnds[ii]][rowEnds[jj]][0] == currPreference:

                    if columnMatchingLookUp.get(jj) == None:
                        matched = True
                        #columnMatchingLookUp[jj] = [ii, preferenceStructure[columnEnds[ii]][rowEnds[jj]][0], preferenceStructure[columnnEnds[ii]][rowEnds[jj]][1]]
                        rowMatchingLookUp[ii] = jj
                        columnMatchingLookUp[jj] = ii

                    else:
                        currPreference += 1
                
                jj += 1

                if jj >= a:
                    jj = 0
        nm2Pairs = []
        for ii in range(0, a):
            nm2Pairs.append(columnEnds[ii], rowEnds[rowMatchingLookUp[ii]])
        return nm2Pairs
            
    def updateMatching(rowMatchingLookUp, columnMatchingLookUp, preferenceStructure, nm1Pairs, nm2Pairs):
        #updates current matching to reflect addition of new pairs
        #update for nm1 pairs
        for ii in range(0, len(nm1Pairs)):
            rowIndex = nm1Pairs[ii][0]
            columnIndex = nm1Pairs[ii][1]
            rowMatchingLookUp[rowIndex] = columnIndex
            columnMatchingLookUp[columnIndex] = [rowIndex, preferenceStructure[rowIndex][columnIndex][0], preferenceStructure[rowIndex][columnIndex][1]]
        
        for ii in range(0, len(nm2Pairs)):
            rowIndex = nm2Pairs[ii][0]
            columnIndex = nm2Pairs[ii][1]
            rowMatchingLookUp[rowIndex] = columnIndex
            columnMatchingLookUp[columnIndex] = [rowIndex, preferenceStructure[rowIndex][columnIndex][0], preferenceStructure[rowIndex][columnIndex][1]]

        return [rowMatchingLookUp, columnMatchingLookUp]

    def PII(preferenceStructure, n, initialMatching):
        #driver method for PII algorithm
        stable = False
        ITER = 0
        nm1Pairs = []
        nm2Pairs = []
        rowMatchingLookUp = {}
        columnMatchingLookUp = {}
        preferenceRDD =  sc.parallelize(preferenceStructure, n)
        [rowMatchingLookUp, columnMatchingLookUp] = initialMatching

        while (not stable):

            nm1Pairs = PII.findNMI(preferenceRDD, columnMatchingLookUp, preferenceStructure)

            if (len(nm1Pairs) <= 0):

                stable = True

            else:

                nm2Pairs = PII.findNM2(rowMatchingLookUp, columnMatchingLookUp, preferenceStructure, nm1Pairs, ITER)
                [rowMatchingLookUp, columnMatchingLookUp] = PII.updateMatching(rowMatchingLookUp, columnMatchingLookUp, preferenceStructure, nm1Pairs, nm2Pairs)
                ITER += 1
                #*****TESTING*****
                #print("Matching in Algo = " + str(rowMatchingLookUp))
                if (Detector2.detectCycle(ITER, n, rowMatchingLookUp, preferenceStructure)):
                    Convergence.logIterations(ITER)
                    del preferenceRDD
                    return {}
                #******TESTING******
            
            nm1Pairs = []
            nm2Pairs = []

        #*****TESTING*****
        del preferenceRDD
        Convergence.logIterations(ITER)

        return rowMatchingLookUp

            
            









        







