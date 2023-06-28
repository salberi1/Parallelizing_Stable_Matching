import copy
SPACING = 4

class Detector:

    matching = {}
    matchingList = []
    pickle = None

    def toString(preferenceStructure, n):
        #converts all stored data to human readable string
        print(Detector.matchingList)
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
                        matrixPrintable += " " + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "0" 
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



    

# class Detector:
#     #class to detect cycles in PII algorithm and output them to text file

#     matching = {}
#     matchings = []

#     def __init__(self, dataDumpName):
#         #initialization
#         matching = {}
#         matchings = []
#         self.dataDump = dataDumpName
#         self.pickle = open(dataDumpName, "w+")

#     # def setMatching(self, matching):
#     #     self.matching = matching

#     # def getMatching(self):
#     #     return self.matching

#     def updateList(self, matching):
#         self.matchings.append(matching)
    
#     # def getLength(self):
#     #     return len(self.matchings)

#     def toString(self, preferenceStructure, n):
#         #converts all stored data to human readable string
#         finalPrintable = "**********************\n"
#         ITER = 0
#         for matching in self.matchings:
#             #for each matching
#             matrixPrintable = "Matching [" + str(ITER) + "]:\n"
#             ITER += 1
#             for ii in range(0, n):
#                 for jj in range(0, n):
#                     if (matching[ii] == jj):
#                         matrixPrintable += "[" + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "]" 
#                     else:
#                         matrixPrintable += " " + str(preferenceStructure[ii][jj][0]) + "," + str(preferenceStructure[ii][jj][1]) + "0" 
#                 matrixPrintable += "\n"

#             finalPrintable += matrixPrintable

#         finalPrintable += "**********************\n"
#         return finalPrintable

#     def dump(self, preferenceStructure, n):
#         #writes string to file
#         string = self.toString(preferenceStructure, n)
#         self.pickle.write(string)

#     def detectCycle(self, ITER, n, currMatching, preferenceStructure):
#         #main driver function called from instance of class
#         print(currMatching)
#         if ITER < 3*n:
#             return False
#         print(Detector.matching == currMatching)
#         print(Detector.matching)
#         print(currMatching)
#         print(ITER)

#         if len(Detector.matchings) == 0:
#             Detector.matching = currMatching
#             Detector.matchings.append(currMatching)
#             print("This is the matching after the append IF: " + str(Detector.matching))
#             print("in IF")
#             return False

#         elif Detector.matching == currMatching:
#             print("In ELIF")
#             self.dump(preferenceStructure, n)
#             self.reset()
#             return True
            
#         else:
#             print("IN ELSE")
#             self.updateList(currMatching)
#             return False
        

#     def reset(self):
#         #reset data structures for next iteration of matching
#         Detector.matching = {}
#         Detector.matchings = []
        
#     def shutdown(self):
#         #close file
#         self.pickle.close()



