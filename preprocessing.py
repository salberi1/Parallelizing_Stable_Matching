import random
from sparkPII import Generator

class Preprocessors:

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

    def rowColMin(preferenceStructure, n):

        def columnMin(rowIndex, columnIndex, deleted):

            minimum = preferenceStructure[rowIndex][columnIndex][0] + preferenceStructure[rowIndex][columnIndex][1]

            for ii in range(0, n):
                if deleted.get(ii) == None:

                    colMin = preferenceStructure[ii][columnIndex][0] + preferenceStructure[ii][columnIndex][1]

                    if colMin < minimum:

                        return False

            return True

        rowColMinimums = []
        matching = []
        deleted = {}
        rowMatchingLookUp = {}
        columnMatchingLookUp = {}

        while(len(matching) < n):

            for ii in range(0, n):

                if deleted.get(ii) == None:

                    rowMin = 3 * n
                    colIndex = -1
                    for jj in range(0, n):

                        if deleted.get(jj + n) == None:
                           
                            currSum = preferenceStructure[ii][jj][0] + preferenceStructure[ii][jj][1]
                                
                            if currSum < rowMin:
                                
                                rowMin = currSum
                                colIndex = jj
                        
                    if (columnMin(ii, colIndex, deleted)):

                        rowColMinimums.append([ii, colIndex])
                        matching.append([ii, colIndex])
                        deleted[ii] = True
                        deleted[colIndex + n] = True

        for indices in matching:
                
            ii = indices[0]
            jj = indices[1]
            columnMatchingLookUp[jj] = [ii, preferenceStructure[ii][jj][0], preferenceStructure[ii][jj][1]]
            rowMatchingLookUp[ii] = jj
        print(matching)
        return [rowMatchingLookUp, columnMatchingLookUp]

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
    
    def quickNM2Selector(rowEnds, columnEnds):

        return

#test
n = 10
preferenceStructure = Generator.generatePreference(n)
[rowMatch, colMatch] = Preprocessors.rowColMin(preferenceStructure, n)
print("##")
print(len(colMatch))
print(colMatch)

