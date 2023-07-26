import copy
import statistics
SPACING = 4
SPLIT = "  "

class Detector2:

    matching = {}
    matchingList = []
    convergedMatchingList = []
    pickle = None
    cucumber = None
    kimchi = None


    def toString(preferenceStructure, n):
        #converts all stored data to human readable string
        CASE = Convergence.Case
        if CASE == 0:
            testName = "Random"
        elif CASE == 1:
            testName = "GS"
        else:
            testName = "RC"
        finalPrintable = "***********~" + testName + "~***********\n"
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
        for item in Detector2.convergedMatchingList:
            del item
        Detector2.matchingList = []
        Detector2.convergedMatchingList = []
    
    def shutdown():

        Detector2.pickle.close()
        Detector2.cucumber.close()
        Detector2.kimchi.close()

    def detectCycle(ITER, n, currMatching, preferenceStructure):

        #TEST ******
        # print("*************")
        # print("iteration: " + str(ITER))
        # print("matching: " + str(Detector2.matching))
        # print("current matching: " + str(currMatching))
       # print("matching list content = " + str(Detector2.matchingList))
        #test ******
        if ITER < 5 * n:
            Detector2.matchingList.append(copy.deepcopy(currMatching))
            return False

        elif ITER == 5 * n:
            Detector2.matchingList.append(copy.deepcopy({0:0}))
            Detector2.matching = copy.deepcopy(currMatching)
            Detector2.matchingList.append(Detector2.matching)
            return False
        #COMMENTED OUT TO SAVE LOGGING TIME      
        elif currMatching != Detector2.matching:
            Detector2.matchingList.append(copy.deepcopy(currMatching))
            return False

        else:  
            Detector2.dump(preferenceStructure, n)
            Detector2.convergenceDump(preferenceSTructure, n)
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

    def logState(currMatching):

        Detector2.convergedMatchingList.append(copy.deepcopy(currMatching))

        return

    def convergenceDump(preferenceStructure, n):

        def toString(currMatching, preferenceStructure, n):

            matrixPrintable = "\n**********************\n"

            for ii in range(0, n):
                for jj in range(0, n):
                    if (currMatching.get(ii) == jj):
                        matrixPrintable += " [" + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "] " 
                    else:
                        matrixPrintable += "  " + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "  " 
                matrixPrintable += "\n\n"
            
            matrixPrintable += "**********************\n"
            return matrixPrintable

        finalPrintable = ""
        for matching in Detector2.convergedMatchingList:

            finalPrintable += toString(matching, preferenceStructure, n)

        Detector2.kimchi.write(finalPrintable)

        return

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
        string = "[n = " + str(n) + "] Mean: " + str(statistics.mean(data)) + " Median: " + str(statistics.median(data)) + " Most: " + str(max(data)) + " STD :" + str(statistics.stdev(data)) + " Cycles: " + str(Convergence.cyclesRandom) + "\n"
        string2 = "[n = " + str(n) + "] Mean: " + str(statistics.mean(data2)) + " Median: " + str(statistics.median(data2)) + " Most: " + str(max(data2)) + " STD :" + str(statistics.stdev(data2)) + " Cycles: " + str(Convergence.cyclesGS) + "\n"
        #string3 = "[n = " + str(n) + "] Mean: " + str(statistics.mean(data3)) + " Median: " + str(statistics.median(data3)) + " Most: " + str(max(data3)) + " STD :" + str(statistics.stdev(data3)) + " Cycles: " + str(Convergence.cyclesRC) + "\n"
        Convergence.pickle.write(string + string2)

    def reset():
        Convergence.iterationsListRandom = []
        Convergence.iterationsListGS = []
        Convergence.iterationsListRC = []
        Convergence.cyclesRandom = Convergence.cyclesGS = Convergence.cyclesRC = 0

    def shutdown():
        Convergence.pickle.close()

class Enumerate:
    #      ~* DATA STRUCTURE FORMATS *~
    #men:  [[matched, preference, prefernceList], ...]
    #women [[currMan, preferenceList], ...]
    #man preferenceList: [first preference women, second preference women, ...]
    #women preferenceList: [preference for man i, preference for man j, ...]
    pickle = None
    brokenMan = -1

    def toString(women, preferenceStructure, n):

        printable = "***************\n"

        for ii in range(0, n):

            for jj in range(0, n):

                LEFT = preferenceStructure[ii][jj][0]
                RIGHT = preferenceStructure[ii][jj][1]

                if women[jj][0] == ii:
                    #set as matched pair
                    printable += " [" + str(LEFT) + "," + str(RIGHT) + "] "
                else:
                    #otherwise nothing
                    printable += "  " + str(LEFT) + "," + str(RIGHT) + "  "
            
            printable += "\n"

        printable += "***************\n"
        return printable

    def dump(women, preferenceStructure, n):
        #writes matching to file
        printable = Enumerate.toString(women, preferenceStructure, n) 
        Enumerate.pickle.write(printable)
        return

    def accept(man, woman, men, women):
        #checks to see if woman will switch engagement to current man
        if (women[woman][0] < 0):
            #if the woman is unmatched, then take this man
            women[woman][0] = man
            men[man][0] = True
            men[man][1] += 1
            return True
        #otherwise, get the preference of the new man and the man she is matched with
        newManPreference = women[woman][1][man]
        oldMan = women[woman][0]
        oldManPreference = women[woman][1][oldMan]

        if newManPreference > oldManPreference:
        #if she does not prefer this man no change occurs
            return False
        #otherwise she prefers this man, update the matching
        # for ii in range(0, len(women)):

        #     if (women[ii][0] == man):
        #         women[ii][0] = -1

        if (oldMan != Enumerate.brokenMan):
            men[oldMan][0] = False
        men[man][0] = True
        men[man][1] += 1
        women[woman][0] = man

        print(men)

        return True
    
    def propose(man, men, women, n):
        #makes proposals on man i's preference list till he is accepted.
        matched = False
        print(man)
        print(Enumerate.brokenMan)
        while(not matched):
            if (men[man][1] >= n):
                break
            preference = men[man][1]
            woman = men[man][2][preference]
            print(woman)

            if (Enumerate.accept(man, woman, men, women)):
                matched = True
                break

            men[man][1] += 1
        
        return matched

    def stableMatching(men, women, n, index):
        #finds stable matching from unmatched men
        while(True):
            unmatchedMen = []

            for man in range(0, n):
            #find all men who are not matched and add them to a list
                if (not men[man][0]):
                    if man < index:
                        return [False, men, women]

                    unmatchedMen.append(man)

            if (len(unmatchedMen) <= 0):
                break
            
            for man in unmatchedMen:
            #for the unmatched men, attempt to find a stable marraige
                if (not Enumerate.propose(man, men, women, n)):
                #propose only returns false when a man exhaust his preference list and not stable matching is found
                    return [False, men, women]

        return [True, men, women]

    def breakMarraige(man, men, women, n):

        men[man][0] = False
        Enumerate.brokenMan = man
        if (not Enumerate.propose(man, men, women, n)):
            return [False, men, women]

        return [True, men, women]

    def fileInitializeMatching():

        cucumber = None

        while True:
            try:
                n = int(input("n:= "))
                fileName = str(input("FILE NAME: "))
                cucumber = open(fileName, "r")
                break

            except:
                print("FILE DOES NOT EXIST")
                return

        men = [[int for i in range(3)] for j in range(n)]
        women = [[int for i in range(2)] for j in range(n)]
        preferenceStructure = [[[int for i in range(2)] for j in range(n)] for k in range(n)]
        pairs = []

        lines = cucumber.read().split('\n')
        for line in lines:
            pairs.extend(line.split(SPLIT))

        print(pairs)
        
        for ii in range(0, n):

            preferenceList = [int for i in range(0, n)]

            for jj in range(0, n):

                pair = pairs[ii * n + jj].split(',')
                leftValue = int(pair[0])
                preferenceStructure[ii][jj][1] = int(pair[1])
                preferenceStructure[ii][jj][0] = leftValue
                preferenceList[leftValue - 1] = jj

            men[ii] = [False, 0, copy.deepcopy(preferenceList)]

        for jj in range(0, n):

            woman = jj + 1
            preferenceList = [int for i in range(0, n)]

            for ii in range(0, n):

                preferenceList[ii] = preferenceStructure[ii][jj][1]

            women[jj] = [-1, copy.deepcopy(preferenceList)]

        Enumerate.showPreference(preferenceStructure, n)

        return [men, women, preferenceStructure, n]

    def usrInitalizeMatching():
        #takes user input and generates a preference structure,
        #and matching dictionaries
        n = int(input("n := "))
        men = [[int for i in range(3)] for j in range(n)]
        women = [[int for i in range(2)] for j in range(n)]
        preferenceStructure = [[[int for i in range(2)] for j in range(n)] for k in range(n)]
        
        for ii in range(0, n):

            man = ii + 1
            preferenceList = [int for i in range(0, n)]

            for jj in range(0, n):

                string = "m_" + str(man) + " preference for w_" + str(jj + 1) + ":= "
                preference = int(input(string))
                preferenceStructure[ii][jj][0] = preference
                preferenceList[preference - 1] = jj

            men[ii] = [False, 0, copy.deepcopy(preferenceList)]

        for jj in range(0, n):

            woman = jj + 1
            preferenceList = [int for i in range(0, n)]

            for ii in range(0, n):

                string = "w_" + str(woman) + " preference for m_" + str(ii + 1) + ":= "
                preference = int(input(string))
                preferenceStructure[ii][jj][1] = preference
                preferenceList[ii] = preference - 1

            women[jj] = [-1, copy.deepcopy(preferenceList)]

        #testing
        Enumerate.showPreference(preferenceStructure, n)

        return [men, women, preferenceStructure, n]

    def initalizeMatching():

        while True:

            try:
                initType = input("USR INITIALIZATION [y/n]? ")

                if initType == "y":
                    return Enumerate.usrInitalizeMatching()
                else:
                    return Enumerate.fileInitializeMatching()
            except:
                print("ERROR ENUMERATING MATCHING")
                return

    def quickStableMatching():
        #for a given preference structure, enumerates some of
        #the stable matchings 

        try:
            [startingMen, startingWomen, preferenceStructure, n] = Enumerate.initalizeMatching()
            [matched, startingMen, startingWomen] = Enumerate.stableMatching(startingMen, startingWomen, n, 0)
            Enumerate.dump(startingWomen, preferenceStructure, n)
        except:
            return

        for ii in range(0, n):

            men = copy.deepcopy(startingMen)
            women = copy.deepcopy(startingWomen)
            savedMen = copy.deepcopy(men)
            savedWomen = copy.deepcopy(women)

            for jj in range(ii, n):
                print(ii, jj)
                [possible, men, women] = Enumerate.breakMarraige(jj, men, women, n)
                if (not possible):
                    men = copy.deepcopy(savedMen)
                    women = copy.deepcopy(savedWomen)
                    print("BM FAILED")
                    continue
                [matched, men, women] = Enumerate.stableMatching(men, women, n, ii)

                if (matched):
                    Enumerate.dump(women, preferenceStructure, n)
                    savedMen = copy.deepcopy(men)
                    savedWomen = copy.deepcopy(women)
                    print("MATCHING SUCCEEDED")
                else:
                    men = copy.deepcopy(savedMen)
                    women = copy.deepcopy(savedWomen)
                    print("MATCHING FAILED")

    def showPreference(preferenceStructure, n):

        for ii in range(0, n):
            for jj in range(0, n):
                LEFT = preferenceStructure[ii][jj][0]
                RIGHT = preferenceStructure[ii][jj][1]
                string = " " + str(LEFT) + "," + str(RIGHT) + " "
                print(string, end = " ")
            print("\n")

class Correctness:

    pickle = None

    def isStable(matchingTupple, preferenceStructure, n):
        #matchingTupple = [matching, ITER]
        [matching, ITER] = matchingTupple

        if ITER <= 3 * n:

            return False

        elif (matching == {}):

            return False

        writtable = "****************\n"

        for ii in range(0, n):

            for jj in range(0, n):

                if (matching.get(ii) == jj):

                    writtable += "[" + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "]" 

                else:

                    writtable += " " + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + " " 
                
            writtable += "\n\n"

        Correctness.pickle.write(writtable)

        return True



#TEST
# Enumerate.pickle = open("stableMatchings.txt", "w+")
# Enumerate.quickStableMatching()
# Enumerate.pickle.close()

# NOTES:
# need to add condition in the propose function,
# not just that women is free but she must prefer
# new man to old man that she was matched with










