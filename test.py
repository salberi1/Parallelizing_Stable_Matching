from sparkPII import *
from framework import *
from preprocessing import *

n = 0
STEP = 2
BOUND = 10
TRIALS = 100
ITER = 0
dest1 = "cyclelogs.txt"
dest2 = "performanceData.txt"
Detector2.pickle = open(dest1, "w+")
Convergence.pickle = open(dest2, "w+")

while (n < BOUND):

    n += STEP

    print(n)
    ITER = 0

    for iterations in range(0, TRIALS):

        ITER += 1

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
        print(ITER)

    Convergence.dump(n)
    Convergence.reset()

Detector2.shutdown()
Convergence.shutdown()



        

