#This is an implementation of Colin's code
#The goal is to implement the same elements that are used for his cycle detection method
#At the 3nth iteration we want to save that current matching and create a chain using nm1-pairs
#With each iteration we check if the matching from the 3nth step is the same has the current step that we're on so that we can stop the iteration
#We also want to check for the root, which is a pair(s) within each matching that remains the same throughout the whole cycle detection 
#After the first iteration, if an nm1-pair is in the same row or column as any of the last nm1 pairs in the chain add it to that existing chain, otherwise start another chain
#Once the cuurent matching is the same as the 3nth matching, a cycle has been detected and the iteration stops
#If the first pair of one chain is the same as the last pair of another chain, link those two chains together and that will form one nm1-cycle
#Then once that's complete, choose every other nm1-pair within that cycle and add it to the matching along with the root pairs and that should create a stable matching

from framework import *


class cycleDetection:

    def nm1Cycle(nm1pairs, ITER, n, currentMatching):
        
        nm1Chains = []
        nm1cycle = []
        newMatchings = []
        #Since originially, nm1pairs are formatted like this [[rowIndex, colIndex],[rowIndex, colIndex],...] (a list in a list) 
        #and nm1-chains are basically a chain of nm1-pairs, therefore the nm1Chains is going to be a list of those nm1-chains]                                  
        #it'll be formatted like this [[[rowIndex, colIndex],[rowIndex, colIndex],...],[[rowIndex, colIndex],[rowIndex, colIndex],...],...]     
        #where nm1Chains[0] is one nm1-chain and nm1-chain[1] is the second nm1-chain and so on


        if ITER == 3*n:
            nm1Chains.append(copy.deepcopy(nm1pairs))           #after the 3nth iteration, add the nm1pairs to the chain

        elif currentMatching != Detector2.matching:                        
            tempnm1pairs = copy.deepcopy(nm1pairs)          #save the nm1pairs sent by the arguments to a temp nm1pair variables, since we'll be modifying (removing) so of the nm1 pairs 

            last = len(nm1Chains)
        #after 3nth and for every subsequent step, add an nm1 pair to the last existing chain existing chain if it's in the same row or column last,most recent nm1-pariin the chain
            for j in range (0, len(tempnm1pairs)):
                if (tempnm1pairs[j][0] == nm1Chains[last-1][len(nm1Chains[last-1])-1][0] or tempnm1pairs[j][1] == nm1Chains[last-1][len(nm1Chains[last-1])-1][1]) and tempnm1pairs[j] != nm1Chains[last-1][len(nm1Chains[last-1])-1]:
                    nm1Chains[i][n].insert(n+1, tempnm1pairs[j])
                    tempnm1pairs.pop(j)

            if len(tempnm1pairs) > 0:                   #otherwise, create a new chain
                nm1Chains.append(copy.deepcopy(nm1Chains))

            del tempnm1pairs        #delete temp nm1pair vaiable to relieve some memory


        else:
            for i in range (0, len(nm1Chains)):              #once the algorithm reached a cycle, check if any first pair of one chain is the same as the last pair of another (and vice versa) and if so, link them to form a nm1Cycle 
                for r in range(i+1, len(nm1Chains)):
                    if nm1Chains[i][0] == nm1Chains[r][len(nm1Chains[r])-1] and nm1Chains[i][len(nm1Chains[i])-1] == nm1Chains[r][0]:
                        nm1Chains[r].pop(0)
                        nm1Chains[r].pop(len(nm1Chains[r])-1)
                        nm1Chains[i].extend(nm1Chains[r])
                        nm1cycle = nm1Chains[i]
                        break

            del nm1Chains               #Once we found the cycle we won't need the chains for more
            
            for o in range(0, len(nm1cycle), 2):            #now choose every other pair in cycle and those pairs will be added to the new matching
                newMatchings.append(nm1cycle[o])

                
        return newMatchings


