import copy
import statistics
SPACING = 4

class Detector:

    matching = {}
    matchingList = []
    pickle = None

    def toString(preferenceStructure, n):
        #converts all stored data to human readable string
        finalPrintable = "**********************\n"
        ITER = 0
        for matching in Detector.matchingList:
            #for each matching
            matrixPrintable = "Matching [" + str(ITER) + "]:\n"
            ITER += 1
            for ii in range(0, n):
                for jj in range(0, n):
                    if (matching[ii] == jj):
                        matrixPrintable += "[" + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "]" 
                    else:
                        matrixPrintable += " " + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + " " 
                matrixPrintable += "\n"

            finalPrintable += matrixPrintable

        finalPrintable += "**********************\n"
        return finalPrintable

    def dump(preferenceStructure, n):
        #writes string to file
        string = Detector.toString(preferenceStructure, n)
        Detector.pickle.write(string)

    def reset():

        Detector.matching = {}
        Detector.matchingList = []
    
    def shutdown():

        Detector.pickle.close()

    def detectCycle(ITER, n, currMatching, preferenceStructure):

        if ITER < 3 * n:
            return False
        #TEST ******
        # print("*************")
        # print("iteration: " + str(ITER))
        # print("matching: " + str(Detector.matching))
        # print("current matching: " + str(currMatching))
        # print("matching list content = " + str(Detector.matchingList))
        #test ******
        if len(Detector.matchingList) <= 0:
            Detector.matching = copy.deepcopy(currMatching)
            Detector.matchingList.append(Detector.matching)
            return False
        
        elif currMatching != Detector.matching:
           Detector.matchingList.append(copy.deepcopy(currMatching))
           return False

        else:  
            Detector.dump(preferenceStructure, n)
            Detector.reset()
            return True

class Detector2:

    matching = {}
    matchingList = []
    pickle = None

    def toString(preferenceStructure, n):
        #converts all stored data to human readable string
        finalPrintable = "**********************\n"
        ITER = 0
        for matching in Detector2.matchingList:
            if matching == {0:0}:
                finalPrintable += "~--CYCLE BEGINS--~\n"
            else:
                matrixPrintable = "Matching [" + str(ITER) + "]:\n"
                ITER += 1
                for ii in range(0, n):
                    for jj in range(0, n):
                        if (matching.get(ii) == jj):
                            matrixPrintable += "[" + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "]" 
                        else:
                            matrixPrintable += " " + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + " " 
                    matrixPrintable += "\n"

                finalPrintable += matrixPrintable

        finalPrintable += "**********************\n"
        return finalPrintable

    def dump(preferenceStructure, n):
        #writes string to file
        string = Detector2.toString(preferenceStructure, n)
        Detector2.pickle.write(string)

    def reset():

        Detector2.matching = {}
        for item in Detector2.matchingList:
            del item
        Detector2.matchingList = []
    
    def shutdown():

        Detector2.pickle.close()

    def detectCycle(ITER, n, currMatching, preferenceStructure):

        #TEST ******
        # print("*************")
        # print("iteration: " + str(ITER))
        # print("matiching: " + str(Detector2.matching))
        # print("current matching: " + str(currMatching))
       # print("matching list content = " + str(Detector2.matchingList))
        #test ******
        
        if ITER < 3 * n:
            Detector2.matchingList.append(copy.deepcopy(currMatching))

            return False

        elif ITER == 3 * n:
            #Case = Convergence.Case
            #if Case == 0:
                #Convergence.cyclesRandom += 1
            #elif Case == 1:
                #Convergence.cyclesGS += 1
            #else:
                #Convergence.cyclesRC += 1
            #return True
           # Detector2.matchingList.append(copy.deepcopy({0:0}))
            Detector2.matching = copy.deepcopy(currMatching)
            #Detector2.matchingList.append(Detector.matching)
            return False
                
        elif currMatching != Detector2.matching:
            #Detector2.matchingList.append(copy.deepcopy(currMatching))
            return False

        else:  
            #Detector2.dump(preferenceStructure, n)
            #*****calculate cycles for each method *****
            Case = Convergence.Case
            if Case == 0:
                Convergence.cyclesRandom += 1
            elif Case == 1:
                Convergence.cyclesGS += 1
            else:
                Convergence.cyclesRC += 1
            #***********
            return True

class Convergence:
    
    Case = 0
    pickle = None
    cyclesRandom = 0
    cyclesGS = 0
    cyclesRC = 0
    iterationsListRandom = []
    iterationsListGS = []
    iterationsListRC = []
    

    def logIterations(iterations):
        if Convergence.Case == 0:
            Convergence.iterationsListRandom.append(iterations)
        elif Convergence.Case == 1:
            Convergence.iterationsListGS.append(iterations)
        else:
            Convergence.iterationsListRC.append(iterations)

    def dump(n):
        data = Convergence.iterationsListRandom
        data2 = Convergence.iterationsListGS
        data3 = Convergence.iterationsListRC
        string = "[n = " + str(n) + "] Mean: " + str(statistics.mean(data)) + " Median: " + str(statistics.median(data)) + " Most: " + str(max(data)) + " STD :" + str(round(statistics.stdev(data), 2)) + " Cycles: " + str(Convergence.cyclesRandom) + "\n"
        string2 = "[n = " + str(n) + "] Mean: " + str(statistics.mean(data2)) + " Median: " + str(statistics.median(data2)) + " Most: " + str(max(data2)) + " STD :" + str(round(statistics.stdev(data2), 2)) + " Cycles: " + str(Convergence.cyclesGS) + "\n"
        string3 = "[n = " + str(n) + "] Mean: " + str(statistics.mean(data3)) + " Median: " + str(statistics.median(data3)) + " Most: " + str(max(data3)) + " STD :" + str(round(statistics.stdev(data3), 2)) + " Cycles: " + str(Convergence.cyclesRC) + "\n"
        Convergence.pickle.write(string + string2 + string3)

    def reset():
        Convergence.iterationsListRandom = []
        Convergence.iterationsListGS = []
        Convergence.iterationsListRC = []
        Convergence.cyclesRandom = Convergence.cyclesGS = Convergence.cyclesRC = 0

    def shutdown():
        Convergence.pickle.close()




