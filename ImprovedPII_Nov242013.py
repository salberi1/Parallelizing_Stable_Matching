#!/usr/bin/env python
# coding: utf-8

# In[10]:


#Importing libraries
import findspark
import pyspark
import array
import random


# In[15]:


trials = 1000
nMin = 10 
nInc = 10
nMax = 100 
c = 6

#prints various matrices
print_output = False

#true,sequential,hadoop,exhaustive
create = "true" 

#smart initiation improvement
smartinit = False

#cuts off first phase after n iterations, starts transposed phase
init2 = False

#cycle detection code
cyc_Detect = False

ps = open("data.txt", "w")

#total count of all trials
contTotal = 0

#count of successful trials
togTotal = 0

#for smartinit 1 and 2 (just leave this at 2)
test = 2
totalSteps = [0] * test
totals = [0] * test

for n in range(nMin, nMax + 1, nInc):
    count = 0  # count per n value
    longest = 0  # longest cycle
    mostSteps = [0, 0]
    counts = [0] * test
    steps = [0] * test
    
    for t in range(trials):
        if t % 1000000 == 0 and t != 0:
            ps.write("million " + str(counts[0] + counts[1]) + "\n")
        initGraph = None
        initiMatch = [0] * n
        initjMatch = [0] * n
        
        if create == "true":  # create random preference list and initial match
            initGraph = create(n)  # create random preference list
            
            # initiation step    graph[i][j][l,r,type,pointer]
            for i in range(n - 1):  # (i,i) trades pointers with a random (i,j) i<=j<n
                j = random.randint(i, n-i)  # (i,i) points to (i+1,j)   (i,j) points to (i+1,i)
                initGraph[i][i][3] = j
                initGraph[i][j][3] = i

            for i in range(n):  # find the match for each row
                j = initGraph[0][i][3]
                for k in range(1, n):  # go through the chain of pointers
                    j = initGraph[k][j][3]
                initGraph[i][j][2] = 1  # 1 = match
                initiMatch[i] = j
                initjMatch[j] = i 
                # initiation done
        
        elif create == "sequential":  # inputs an existing preference list and initial match from init.txt
            initGraph = getInit(n)  # gets the preference list and initial match
            for i in range(n):  # sets up the arrays of matches
                for j in range(n):
                    if initGraph[i][j][2] == 1:
                        initiMatch[i] = j
                        initjMatch[j] = i
                        
        elif create == "hadoop":  # inputs an existing preference list and initial match from part-00000 (file from hadoop)
            initGraph = getHadoop(n)  # gets the preference list and initial match
            for i in range(n):  # sets up the arrays of matches
                for j in range(n):
                    if initGraph[i][j][2] == 1:
                        initiMatch[i] = j
                        initjMatch[j] = i
                        
        else:
            print("misspelled")
            return 
            # done creating preferences and initial matching
            
        if print_flag:
            print(initGraph)
            print(n, "init.txt")
            
        graph = [[[0] * 4 for _ in range(n)] for _ in range(n)]
        iMatch = [0] * n  #(i,iMatch[i]) is a match
        jMatch = [0] * n  #(jMatch[j],j) is a match
        stepNum = 0

        cycled = False
        matchFromCycle = False
        past3 = False
        repeats = [[False] * n for _ in range(n)]  #matrix that makes sure nm1s are not repeated in chains
        
        for a in range(2):
            #this loop exists because running smartinit twice, once on l and once on r values, finds a stable match with very high probability
            #I was going to run the two trials in tandem, but never got around to it
            stepNum = 0

            if a == 0 and not init2:  #if we're not running smartinit twice, just do this loop once
                a = 1

            for i in range(n):  #go back to initial graph
                for j in range(n):
                    for k in range(4):
                        graph[i][j][k] = initGraph[i][j][k]
                iMatch[i] = initiMatch[i]
                jMatch[i] = initjMatch[i]

                
            if smart_init:  #mart initiation overrides the current initial match
                newMatches = None
                if a == 0:
                    newMatches = smartInit(graph, n, 1)  #runs on r values
                else:
                    newMatches = smartInit(graph, n, 0)  #runs on l values

                iMatch, jMatch = newMatches[0], newMatches[1]

                if print_flag:  #print the initial matching
                    for i in range(n):
                        print("i:", str(i), " ", str(iMatch[i]), "\n")
                        
            stable = True
            firstMatch = [0] * n
            cycle = [[0] * n for _ in range(3 * n + 1)]
            nm1s = []
            parity = []
            roots = [True] * n
            
            for i in range(n):   #start them all as true
                roots[i] = True
                
            for s in range(c * n):  #iteration loop
                if s == 3 * n:
                    past3 = True
                if past3 and s < 6 * n + 1 and not cycled and cyc_detect:  #if s>=3*n, start looking for cycles. the matrix only has 3*n slots
                    for i in range(n):
                        cycle[s - 3 * n][i] = iMatch[i]
        
                    same = True
                    if s == 3 * n:  #don't check for cycles on the first iteration
                        same = False
                    for i in range(n):  #check for cycles
                        if cycle[s - 3 * n][i] != cycle[0][i]:
                            same = False
                            roots[i] = False  #check for pairs that don't change
        
                    if same: #//if there's a cycle
                        cycled = True
                        if s - 3 * n > longest:  #kep track of longest cycle
                            longest = s - 3 * n
                            #out of past3
                            
                if cycled and not matchFromCycle and cyc_Detect: #if a cycle was detected, and haven't been here before
                    matchFromCycle = True
                    
                    for i in range(n):  #reset the matches
                        iMatch[i] = -1
                        jMatch[i] = -1
                    
                    for i in range(len(nm1s)):  #check if we need to merge the lists together
                        nm1 = nm1s[i]
                        iValue = nm1[0][0]
                        jValue = nm1[0][1]
                        for j in range(len(nm1s)):   #HERE Syntax gets weird with .get function 
                            nm1Check = nm1s[j]
                            if i != j and (iValue == nm1Check[len(nmCheck) - 1][0] or jValue == nm1Check[len(nm1Check) -1][1]):  #if we do need to merge
                                if len(nm1Check) > 1 and (nm1Check[len(nm1Check) - 2][0] == iValue and nm1Check[len(nm1Check) -1][0] == iValue or nm1Check[len(nm1Check) - 2][1] == jValue and nm1Check[len(nm1Check) - 1][1] == jValue):
                                    #if we need to change parity 2-1
                                    parity.pop(j)
                                    parity.insert(j, (len(nm1Check) - 1) % 2)
                                    
                                elif len(nm1Check) > 1 and (nm1Check[len(nm1Check) - 1][0] == iValue and nm1[1][0] == iValue or nm1Check[len(nm1Check) - 1][1] == jValue and nm1[1][1] == jValue):
                                    parity.pop(j)
                                    parity.insert(j,len(nm1Check) % 2)
                                    
                                if parity.get(i) != -1:
                                    # Update the parity of the merged list
                                    # print("updating parity in merge")
                                    par = (parity[i] + len(nm1Check)) % 2
                                    parity.pop(j)
                                    parity.insert(j, par)
                                    
                                while len(nm1) != 0:
                                    # put nm1Check2 on the end of nm1Check
                                    nm1Check.append(nm1.pop(0))

                                nm1s.pop(i)
                                parity.pop(i)
                                i -= 1  # now we have to go back a chain
                                break
                                
                    if len(nm1s) > 1:  # check if we need to merge dead ends
                        for i in range(len(nm1s)):  # for all chains
                            nm1 = nm1s[i]
                            iValue = nm1[0][0]
                            jValue = nm1[0][1]
                            adjacent = 0
                            for k in range(n):
                                if repeats[iValue][k] and k != jValue:
                                adjacent += 1
                            for k in range(n):
                                if repeats[k][jValue] and k != iValue:
                                adjacent += 1
                            if adjacent < 2:  # if it's a dead end
                                iValue = nm1[-1][0]
                                jValue = nm1[-1][1]
                                for j in range(len(nm1s)):  # check every other chain
                                    nm1Check = nm1s[j]
                                        if j != i and (nm1Check[0][0] == nm1Check[len(nm1Check) - 1][0] or nm1Check[0][1] == nm1Check[len(nm1Check) -1][1]):  # if the chain is a cycle
                                            for k in range(len(nm1Check)):  # check every nm1 for an opening
                                                if iValue == nm1Check[k][0] or jValue == nm1Check[k][1]:  # if there is an opening
                                                    while k + 1 < len(nm1Check):  # add everything after k
                                                        nm1.append(nm1Check.pop(k + 1))
                                                    while len(nm1Check) > 0:  # add everything else
                                                        nm1.append(nm1Check.pop(0))
                                                    parity.pop(i)
                                                    parity.insert(i, 0)
                                                    nm1s.pop(j)
                                                    parity.pop(j)
                                                    break
                                                    
                    if len(nm1s) > 2:  #either something is wrong, or there are three cycles
                        stable = False
                        break
                        
                    for i in range(len(nm1s)):
                        nm1Check = nm1s[i]
                        offset = parity[i]
                        if offset == -1:
                            offset = 0
                        j = 0
                        while 2 * j + offset < len(nm1Check):
                            iMatch[nm1Check[2 * j + offset][0]] = nm1Check[2 * j + offset][1]
                            j += 1
                            
                    for i in range(n):
                        if roots[i]:
                            if iMatch[i] == -1:
                                iMatch[i] = cycle[0][i]
                            else:
                                return
                            
                    badCount = 0
                    for i in range(n):
                        if iMatch[i] == -1:
                            badCount += 1
                        else:
                            jMatch[iMatch[i]] = i
                            
                    if badCount == 1:
                        lasti = -1
                        lastj = -1
                        for i in range(n):
                            if iMatch[i] == -1:
                                lasti = i
                            if jMatch[i] == -1:
                                lastj = i
                        iMatch[lasti] = lastj
                        jMatch[lastj] = lasti
                        
                    elif badCount != 0:
                        stable = False
                        break
                        #done with cycle detection code
                            
                okay = True  #test jMatch before entering the iteration loop
                for i in range(n):
                    for j in range(n):
                        if iMatch[i] == -1 or jMatch[i] == -1:
                            okay = False

                if not okay:  #if there was a problem in jMatch
                    stable = False
                    break
                    
                reg = False  # nm1 starts with r
                if a == 1:
                    reg = True  # nm1 starts with l (the regular way)
                total = False  # nm1 decided with l+r (inefficient but very fair)
                nm2type = 1  # 0=original PII, 1=diagonal, 2=random
                nm2switch = False  # if two nm2's, pick better pair (slight speedup)

                # init everything
                nm1i = [-1] * n  # nm1[i] = j => (i,j) is nm1
                nm1j = [-1] * n  # nm1[j] = i
                nm2geni = [-1] * n  # etc
                nm2genj = [-1] * n
                nm2 = [-1] * n

                for i in range(n):
                    nm1i[i] = -1
                    nm1j[i] = -1
                    nm2geni[i] = -1
                    nm2genj[i] = -1
                    nm2[i] = -1
                    
                if stepNum == 0:  # to see initial match in case of error
                    for i in range(n):
                        firstMatch = iMatch[:]
                        
                # now finally the PII algorithm starts

                # graph[i][j][l r type (0=reg,1=match,2=nm1gen)]
                # find nm1gen: for every row, find the lowest l/r/sum that's unstable
                stable = True
                for i in range(n):
                    lowest = 2 * n + 1  # sentinel
                    ref = -1
                    for j in range(n):
                        if graph[i][j][2] != 1 and reg:  # update nm1gen from last time
                            graph[i][j][2] = 0
                        elif graph[j][i][2] != 1 and not reg:
                            graph[j][i][2] = 0

                        graph[i][j][3] = 1  # initialize valids (used for some nm2 types)
                        if graph[i][j][0] < graph[i][iMatch[i]][0] and graph[i][j][1] < graph[jMatch[j]][j][1] and reg:
                            stable = False
                            if graph[i][j][0] < lowest and not total:  # update lowest
                                lowest = graph[i][j][0]
                                ref = j
                            elif (graph[i][j][0] + graph[i][j][1]) < lowest and total:  # test using l+r instead of l
                                lowest = graph[i][j][0] + graph[i][j][1]
                                ref = j
                        elif graph[j][i][0] < graph[j][iMatch[j]][0] and graph[j][i][1] < graph[jMatch[i]][i][1] and not reg:
                            stable = False
                            if graph[j][i][1] < lowest and not total:
                                lowest = graph[j][i][1]
                                ref = j
                            elif (graph[j][i][0] + graph[j][i][1]) < lowest and total:  # l+r
                                lowest = graph[j][i][0] + graph[j][i][1]
                                ref = j
                
                    if ref != -1 and reg:  # set nm1gen
                        graph[i][ref][2] = 2
                    elif ref != -1 and not reg:
                        graph[ref][i][2] = 2
                    # done finding nm1gen
                    
                if stable:
                    break  # exit the iter loop

                # find nm1: for every column, find the lowest r that's nm1gen
                for j in range(n):
                    lowest = 2 * n + 1  # sentinel
                    ref = -1
                    for i in range(n):
                        if graph[i][j][2] == 2 and reg:  # if nm1gen
                            if graph[i][j][1] < lowest and not total:  # reg nm1
                                lowest = graph[i][j][1]
                                ref = i
                            elif (graph[i][j][0] + graph[i][j][1]) < lowest and total:  # use l+r instead
                                lowest = graph[i][j][0] + graph[i][j][1]
                                ref = i
                        elif graph[j][i][2] == 2 and not reg:  # go by rows first
                            if graph[j][i][0] < lowest and not total:
                                lowest = graph[j][i][0]
                                ref = i
                            elif (graph[j][i][0] + graph[j][i][1]) < lowest and total:
                                lowest = graph[j][i][0] + graph[j][i][1]
                                ref = i

                    if ref != -1 and reg:  # update nm1
                        nm1i[ref] = j
                        nm1j[j] = ref
                    elif ref != -1 and not reg:
                        nm1i[j] = ref
                        nm1j[ref] = j
                        # now we've found nm1

                stepNum += 1

                # update the list of nm1 chains when it's cycling (this section is stable match from cycle code)
                if past3 and not matchFromCycle and cycDetect:  # if it's past 3n iterations (guaranteed to cycle)
                    for i in range(n):
                        if nm1i[i] != -1 and not repeats[i][nm1i[i]]:  # for every new nm1i
                            repeats[i][nm1i[i]] = True
                            added = False
                            for j in range(len(nm1s)):  # find out where it goes
                                nm1Check = nm1s[j]
                                if (nm1Check[len(nm1Check) - 1][0] == i or nm1Check[len(nm1Check) - 1][1] == nm1i[i]) and iMatch[nm1Check[len(nm1Check) - 1][0]] == nm1Check[len(nm1Check) - 1][1]:  # if i or j matches an existing nm1 and the nm1 is a current match
                                    toAdd = [i, nm1i[i]]
                                    nm1Check.append(toAdd)
                                    if len(nm1Check) > 2 and (nm1Check[len(nm1Check) - 2][0] == i and nm1Check[len(nm1Check) - 3][0] == i or nm1Check[len(nm1Check) - 2][1] == nm1i[i] and nm1Check[len(nm1Check) - 3][1] == nm1i[i]):  # if 3 in a row
                                        parity.pop(j)
                                        parity[j].insert(j,len(nm1Check) % 2)  # update parity to pick the middle nm1 in the chain of 3
                                    added = True
                                    break
                            if not added:  # if it didn't go into an existing chain, start a new one
                                nm1 = []
                                toAdd = [i, nm1i[i]]
                                nm1.append(toAdd)
                                nm1s.append(nm1)
                                parity.append(-1)
                # done with the nm1 chain code

                for i in range(n):  # find nm2gen
                    if nm1i[i] != -1:  # nm1i is at (i, nm1i[i])
                        nm2geni[jMatch[nm1i[i]]] = iMatch[i]
                        graph[i][iMatch[i]][2] = 0  # these are no longer matches
                        graph[jMatch[nm1i[i]]][nm1i[i]][2] = 0
                # found nm2gen  
                
                #DOUBLE CHECK
                nm2Count = 0
                if nm2type == 0:  # this is the standard way to find nm2 pairs
                    for i in range(n):
                        if nm2geni[i] != -1:  # if there's an nm2gen in row i
                            if nm1i[i] == -1:  # if there's no nm1 in row i
                                if nm1j[nm2geni[i]] == -1:  # if there's no nm1 in the same col
                                    nm2[i] = nm2geni[i]  # isolated node
                                    nm2Count += 1
                                else:  # then it's a row end
                                    iPlus = i
                                    jPlus = nm2geni[i]
                                    while True:  # find the col end
                                        iPlus = jMatch[jPlus]  # i+,j+ is the match in the same col as the nm2gen
                                        jPlus = nm2geni[iPlus]  # i+,j+ is the next nm2gen
                                        if nm1j[jPlus] == -1:
                                            break  # found the col end
                                        nm2[i] = jPlus
                                        nm2Count += 1
                                        #now we've found nm2
                else:  # the other two nm2types start by calculating invalids
                    for i in range(n):
                        for j in range(n):
                            if nm1i[i] != -1 or nm1j[j] != -1:  # if there's an nm1 in your row/col
                                graph[i][j][3] = 0  # not valid
                                if iMatch[i] == j:  # no longer a match
                                    iMatch[i] = -1
                                    jMatch[j] = -1
                    for i in range(n):
                        for j in range(n):
                            if iMatch[i] != -1 or jMatch[j] != -1:  # if there's a surviving match in your row/col
                                graph[i][j][3] = 0  # not valid

                    jinit = 0
                    firsti = -1
                    firstj = -1
                    for i in range(n):  # make nm2 the diagonal
                        for j in range(jinit, n):
                            if graph[i][j][3] == 1:
                                if firsti == -1:
                                    firsti = i
                                    firstj = j
                                nm2[i] = j
                                nm2Count += 1
                                jinit = j + 1
                                break
                    # now we've found nm2 along diagonal

                    if firsti != -1 and nm2type == 2:  # randomly generate nm2 from valids
                        shuffled = []
                        inc = 0
                        for j in range(n):
                            if graph[firsti][j][3] == 1:
                                shuffled.append(j)
                                inc += 1
                            # now shuffled is a list of the j values
                        for i in range(nm2Count):  # Knuth shuffle the array
                            j = random.randint(i, nm2Count - i)
                            holder = shuffled[i]
                            shuffled[i] = shuffled[j]
                            shuffled[j] = holder


                        inc = 0
                        for i in range(n):  # now assign the random nm2s
                            if graph[i][firstj][3] == 1:
                                nm2[i] = shuffled[inc]
                                inc += 1
                    # found random nm2s
                
                if nm2Count == 2 and nm2switch:  # switches to a better pair of nm2s
                    first = True
                    ref1 = -1
                    ref2 = -1
                    for i in range(n):  # find the two nm2s
                        if nm2[i] != -1 and first:
                            first = False
                            ref1 = i
                        elif nm2[i] != -1:
                            ref2 = i

                    # if the sum of l+r of the two current nm2s is worse than the other possible nm2s, switch
                    if (graph[ref1][nm2[ref2]][0] + graph[ref1][nm2[ref2]][1] + graph[ref2][nm2[ref1]][0] + graph[ref2][nm2[ref1]][1]) < (graph[ref1][nm2[ref1]][0] + graph[ref1][nm2[ref1]][1] + graph[ref2][nm2[ref2]][0] + graph[ref2][nm2[ref2]][1]): 
                        holder = nm2[ref1]
                        nm2[ref1] = nm2[ref2]
                        nm2[ref2] = holder
                        
                for i in range(n):  # update matches in graph and arrays
                    if nm1i[i] != -1:
                        graph[i][nm1i[i]][2] = 1
                        iMatch[i] = nm1i[i]
                        jMatch[nm1i[i]] = i
                    elif nm2[i] != -1:
                        graph[i][nm2[i]][2] = 1  # this is a new match
                        iMatch[i] = nm2[i]
                        jMatch[nm2[i]] = i
                # now we've completed a step
                if init2 and s == 2 * n and a == 0:  # init2 runs the first loop 2n times
                    s = c * n + 5  # set s to exit the a loop
            
            if stable:
                steps[a] += stepNum
                counts[a] += 1
                if not cycled and stepNum > mostSteps[0]:  # update mostSteps
                    mostSteps[0] = stepNum
                elif cycled and stepNum > mostSteps[1]:
                    mostSteps[1] = stepNum
                break  # break in case a == 0
    print("finished n=" + str(n) + ", longest cycle " + str(longest) + " most steps acyclic " + str(mostSteps[0] / n) + " cyclic " + str(mostSteps[1] / n +) + "\n")
    ps.write("n=" + str(n) + " trials:" + str(count) + " " + "\n")  # print out info specific to n value
    ps.write("together:" + str(counts[0] + counts[1]) + "\n")
    togTotal = togTotal + counts[0] + counts[1]
    for i in range(test):  # print out successes in each phase
        ps.write(str(i) + ":" + str(counts[i]) + " " + str((0.0 + steps[i]) / counts[i]) + "\n")
    for i in range(test):  # update the total counts
        totalSteps[i] += steps[i]
        totals[i] += counts[i]
    countTotal += count
    
print("done" + "\n")
ps.write("\n")
ps.write("total:" + str(countTotal) + " " + "\n")  # print out totals
ps.write("together:" + str(togTotal) + "\n")
for i in range(test):
    ps.write(str(i) + ":" + str(totals[i]) + " " + str((0.0 + totalSteps[i]) / totals[i]) + "\n")
cm = open("finished.txt", "w")  # so I know when it finished
cm.write("done")

cm.close()
ps.close()


# In[ ]:




