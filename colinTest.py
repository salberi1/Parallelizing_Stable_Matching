from sparkPII import *
from colinFramework import *
from colinPreprocessing import *

n = 6
STEP = 1
BOUND = 10
TRIALS = 10000
ITER = 0
testName = "CycleData-Alternating-BM-NM1Right2.0"
#dest1 = testName + "cyclelogs.txt"
dest2 = testName + "performanceData.txt"
#Detector2.pickle = open(dest1, "w+")
Convergence.pickle = open(dest2, "w+")

while (n < BOUND):

    n += STEP

    print("[" + str(n) +"]")
    ITER = 0

    for iterations in range(0, TRIALS):

        ITER += 1
        print(n, ITER)

        preferenceStructure = Generator.generatePreference(n)

        for case in range(0, 3):
            
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


            PII.PII(struct, n, initialMatching)
            Detector2.reset()
        
        del preferenceStructure
        del struct

    print("\n")
    Convergence.dump(n)
    Convergence.reset()

Detector2.shutdown()
Convergence.shutdown()



        

