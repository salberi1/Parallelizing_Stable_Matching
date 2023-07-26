from nm1RightDynamicsparkPII import *
from nm1RightDynamicframework import *
from nm1RightDynamicpreprocessing import *

n = 0
STEP = 10
BOUND = 100
TRIALS = 100000
ITER = 0
test = "Performance3.0-DynamicSelect-"
#dest1 = test + "cyclelogs.txt"
#dest2 = test + "convergedState.txt"
#dest3 = test + "sequences.txt"
dest4 = test + "performanceData.txt"
#Detector2.pickle = open(dest1, "w+")
#Detector2.cucumber = open(dest2, "w+")
#Detector2.kimchi = open(dest3, "w+")
Convergence.pickle = open(dest4, "w+")

while (n < BOUND):

    n += STEP

    print("[" + str(n) +"]")
    ITER = 0

    for iterations in range(0, TRIALS):

        ITER += 1
        print(str(n) + " | " + str(ITER))

        preferenceStructure = Generator.generatePreference(n)

        for case in range(0, 2):

            if case == 0:
                struct = copy.deepcopy(preferenceStructure)
                initialMatching = Preprocessors.randomMatching(preferenceStructure, n)
                Convergence.Case = 0
            elif case == 1:
                struct = copy.deepcopy(preferenceStructure)
                initialMatching = Preprocessors.nearStableMatching(preferenceStructure, n)
                Convergence.Case = 1
            else:
                struct = copy.deepcopy(preferenceStructure)
                initialMatching = Preprocessors.rowColMin(preferenceStructure, n)
                Convergence.Case = 2

            Improved.previousUnstablePairs = {} #setting the dictionary to empty
            tupple = PII.PII(struct, n, initialMatching) #checking if the matching outputed is actually stable
            #Detector2.convergenceDump(preferenceStructure, n)
            Detector2.reset()
        
        del preferenceStructure
        del struct

    print("\n")
    Convergence.dump(n)
    Convergence.reset()

#Detector2.shutdown()
Convergence.shutdown()



        

