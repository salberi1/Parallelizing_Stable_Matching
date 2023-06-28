from sparkPII import *
from framework import *
from preprocessing import *

n = 20
STEP = 10
BOUND = 100
TRIALS = 1000
ITER = 0
dest1 = "cyclelogs.txt"
dest2 = "performanceData.txt"
Detector2.pickle = open(dest1, "w+")
Convergence.pickle = open(dest2, "w+")

while (n < BOUND):

    n += STEP
    print(n)   
    for iterations in range(0, TRIALS):

        preferenceStructure = Generator.generatePreference(n)
        ITER+=1
        print(ITER)
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
        
            print(" ")
            PII.PII(struct, n, initialMatching)
            Detector2.reset()

    del preferenceStructure
    del struct
    Convergence.dump(n)
    
    #ITER += 1
    #print(n, ITER)



Detector2.shutdown()



        

