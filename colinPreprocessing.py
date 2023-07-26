import random

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

class Improved:

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
                    currRightValue = currColumnMatching[key][2]
                    break
            #for each entry in the row, check if its LEFT value is smaller than the current matching
            oldSum = currLeftValue + currRightValue
            for jj in range(0, len(row)):

                if (row[jj][0] < currLeftValue):
                    #if it is, check to see if its RIGHT value is smaller than the current matching in that column
                    if (row[jj][1] < currColumnMatching[jj][2]):
                        #check to see if the sum of the pair is smaller than the current sum
                        #newSum = row[jj][0] + row[jj][1]
                        newRightValue = row[jj][1]
                        if newRightValue < currRightValue: #changed this from <= to <, perhaps stronger convergence
                            #currLeftValue = row[jj][0]
                            columnIndex = jj

            return [rowIndex, columnIndex]
        
        unstableRowPairs = []
        unstablePairs = []
        unstablePairsDict = {}
        #find minimum unstable pair in each row
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

    def breakMarraige(preferenceRDD, currColumnMatching, preferenceStructure):
        #decriments the preference (left value) of all men in the current matching.
        #if two men are decrimented to the same column, the one with the higher
        #left value is chosen.
        def finder(row, currColumnMatching):
            #worker node function.
            #identifies pair such that LEFT_{new} = LEFT_{old} + 1
            #returns -1 in the case of LEFT_{old} = n
            rowIndex = row[0][2]
            columnIndex = -1
            #find the LEFT value of the current pair in the matching
            for key in currColumnMatching:
                if currColumnMatching:
                    currLeftValue = currColumnMatching[key][1]
                    break
            #for each entry in the row, check to see if the left value
            #satisfies the above condition
            for jj in range(0, len(row)):

                if (row[jj][0] == (currColumnMatching[jj][0] + 1)):
                    columnIndex = jj
                    break

            return [rowIndex, columnIndex]
        
        increasedPreferenceDict = {}
        increasedPreferencePairs = []
        #Find column index of decrimented pair
        increasedPreferenceRowPairs = preferenceRDD.map(lambda row : finder(row, currColumnMatching)).collect()
        #select decrimented pair with higher left value in the case of two pairs being decrimented; !!!! note that this may need to be updated a more sophisticated method in the future
        for ii in range(0, len(increasedPreferenceRowPairs)):

            currColumnIndex = increasedPreferenceRowPairs[ii][1]

            if currColumnIndex > -1:

                currRowIndex = increasedPreferenceRowPairs[ii][0]
                currLeftValue = preferenceStructure[currRowIndex][currColumnIndex][0]
                currRightValue = preferenceStructure[currRowIndex][currColumnIndex][1]

                if increasedPreferenceDict.get(currColumnIndex) == None:

                    increasedPreferenceDict[currColumnIndex] = [currRowIndex, currLeftValue, currRightValue]

                elif currLeftValue < increasedPreferenceDict[currColumnIndex][1]:

                    increasedPreferenceDict[currColumnIndex] = [currRowIndex, currLeftValue, currRightValue]

                elif currLeftValue == increasedPreferenceDict[currColumnIndex][1]:

                    if currRightValue < increasedPreferenceDict[currColumnIndex][2]:

                        increasedPreferenceDict[currColumnIndex] = [currRowIndex, currLeftValue, currRightValue]
                        
                else:
                    continue

        for key in increasedPreferenceDict:
            increasedPreferencePairs.append([increasedPreferenceDict[key][0], key])

        return increasedPreferencePairs 



