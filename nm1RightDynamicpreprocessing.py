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

    previousUnstablePairs = {} #<row, [leftValue, rightValue, ITER, column, wait]>

    def findNMI(preferenceRDD, currColumnMatching, preferenceStructure):
        #finds nm1 pairs and returns as list
        def finder(row, currColumnMatching):
            #worker node function. For each row, 
            #identifies the most minimal unstable pair
            rowIndex = row[0][2]
            columnIndex = -1
            currLeftValue = -1
            #find the LEFT value of the current pair in the matching
            for key in currColumnMatching:
                if currColumnMatching[key][0] == rowIndex:
                    currLeftValue = currColumnMatching[key][1]
                    currRightValue = currColumnMatching[key][2]
                    break
            #for each entry in the row, check if its LEFT value is smaller than the current matching
            #oldSum = currLeftValue + currRightValue
            for jj in range(0, len(row)):

                if (row[jj][0] < currLeftValue):
                    #if it is, check to see if its RIGHT value is smaller than the current matching in that column
                    if (row[jj][1] < currColumnMatching[jj][2]):
                        #check to see if the sum of the pair is smaller than the current sum
                        newRightValue = row[jj][1]
                        if newRightValue < currRightValue:
                            currLeftValue = row[jj][0]
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
            currLeftValue = -1 
            #find the LEFT value of the current pair in the matching
            for key in currColumnMatching:
                if currColumnMatching[key][0] == rowIndex:
                    currLeftValue = currColumnMatching[key][1]
                    break
            #for each entry in the row, check to see if the left value
            #satisfies the above condition
            for jj in range(0, len(row)):

                if (row[jj][0] == (currLeftValue + 1)):
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

        for key in increasedPreferenceDict:
            increasedPreferencePairs.append([increasedPreferenceDict[key][0], key])

        return increasedPreferencePairs 

    def dynammicNM1Selection(preferenceRDD, currColumnMatching, preferenceStructure, ITER):
        #called after 3n iterations. Selects NM1 pairs iff NEW_RIGHT < OLD_RIGHT && NEW_ITER != OLD_ITER + 1
        def finder(row, currColumnMatching):
            #worker node function. For each row, 
            #identifies the most minimal unstable pair
            rowIndex = row[0][2]
            columnIndex = -1
            currLeftValue = -1 #CHANGE MADE HERE
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
        #find minimum unstable pair in each row
        unstableRowPairs = preferenceRDD.map(lambda row : finder(row, currColumnMatching)).collect()   
        #select unstable pair with lower RIGHT value if both in same column
        for ii in range(0, len(unstableRowPairs)):

            columnIndex = unstableRowPairs[ii][1]
            #check to see if the worker nodes were able to identify an unstable pair in its row
            if columnIndex > -1:
                #if so, load relevant information onto stack
                rowIndex = unstableRowPairs[ii][0]
                currLeftValue = preferenceStructure[rowIndex][columnIndex][0]
                currRightValue = preferenceStructure[rowIndex][columnIndex][1]
                prevPair = Improved.previousUnstablePairs.get(rowIndex)
                #check to see if there is a previous NM1 pair in the row
                if (prevPair != None):
                    #if there is check if it has a smaller left value
                    prevLeftValue = prevPair[0]
                    prevITER = prevPair[2]
                    if prevLeftValue < currLeftValue:
                        #if it does, make sure that the NM1 pair was not choosen in the previous iteration. If it was, this may cause cycling.
                        #wait = prevPair[4]
                        if (ITER > (prevITER + 2)):
                            #if all conditions are satisfies, then take the old NM1 Col Index and  RIGHT value
                            columnIndex = Improved.previousUnstablePairs[rowIndex][3]
                            currRightValue = Improved.previousUnstablePairs[rowIndex][1]
                #now complete regular checks to take the NM1 pair with the lowest RIGHT value in a given column
                if unstablePairsDict.get(columnIndex) == None:

                    unstablePairsDict[columnIndex] = [rowIndex, currRightValue]

                elif  currRightValue < unstablePairsDict[columnIndex][1]:
                    #set this as the new best nm1 pair
                    unstablePairsDict[columnIndex] = [rowIndex, currRightValue]

        for key in unstablePairsDict:
            #for the final list of NM1 pairs, append them to a list and check if updates to the dictionary need to be made
            rowIndex = unstablePairsDict[key][0]
            columnIndex = key
            currLeftValue = preferenceStructure[rowIndex][columnIndex][0]
            currRightValue = preferenceStructure[rowIndex][columnIndex][1]

            if (Improved.previousUnstablePairs.get(rowIndex) == None):
                #if there is nothing in the dictionary, we take the first NM1 pair we find
                Improved.previousUnstablePairs[rowIndex] = [currLeftValue, currRightValue, ITER, columnIndex, 2]

            elif (currLeftValue < Improved.previousUnstablePairs[rowIndex][0]):
                #otherwise, if we have found an NM1 pair with a lower LEFT value, we take that one
                Improved.previousUnstablePairs[rowIndex] = [currLeftValue, currRightValue, ITER, columnIndex, 2]

            elif (ITER > Improved.previousUnstablePairs[rowIndex][2] + 2):
                #otherwise, we check to see if we have used one of the dictionary pairs as an NM1 pair.
                #if this is the case, we update when it was taken as a pair, and increase the wait till the next time it
                #may be selected.
                Improved.previousUnstablePairs[rowIndex][2] = ITER
                #Improved.previousUnstablePairs[rowIndex][4] += 1

            unstablePairs.append([rowIndex, columnIndex])


        return unstablePairs

class Patch:

    positions = {}

    def patchCycle(nm1Pairs, preferenceStructure):

        def createNM1Pairs():

            pairs = []

            for key in Patch.positions:

                pair = [key, Patch.positions[key][0][1]]
                pairs.append(pair)

            return pairs
        
        complete = False
        pairs = []

        for ii in range(0, len(nm1Pairs)):

            row = nm1Pairs[ii][0]
            column = nm1Pairs[ii][1]
            leftValue = preferenceStructure[row][column][0]

            if (Patch.positions.get(row) == None):

                Patch.positions[row] = [[leftValue, column]]

            else:
                index = 0
                while (index < len(Patch.positions[row])):
                    
                    currLeftValue = Patch.positions[row][index][0]
                    currColumn = Patch.positions[row][index][1]

                    if (leftValue < currLeftValue):
                        Patch.positions[row].insert(index, [leftValue, column])
                        break
                    elif (column == currColumn):
                        complete = True
                        pairs = createNM1Pairs()

                    index += 1

                    if (index == len(Patch.positions[row])):
                        Patch.positions[row].append([leftValue, column])
                        break

        return [complete, pairs]

