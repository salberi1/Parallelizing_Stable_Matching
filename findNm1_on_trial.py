class PIItemp:

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
        #find minimum unstable pair in each row
        unstableRowPairs = preferenceRDD.map(lambda row : finder(row, currColumnMatching)).collect()
        #select unstable pair with lower RIGHT value if both in same column
        for ii in range(0, len(unstableRowPairs)):

             currColumnIndex = unstableRowPairs[ii][0]

             if currColumnIndex > -1:

                currRowIndex = unstableRowPairs[ii][0]
                leftValue = preferenceStructure[currRowIndex][currColumnIndex][0]

                if unstablePairsDict.get(currColumnIndex) == None:

                    unstablePairsDict[currColumnIndex] = [currRowIndex, leftValue]

                elif  leftValue < unstablePairsDict[currColumnIndex][0]:

                    unstablePairsDict[currColumnIndex] = [currRowIndex, leftValue]