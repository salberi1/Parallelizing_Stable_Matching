from sparkPII import Generator

class GS:

    def initGS(n):
        #initialization of dictionaries
        manDick = {}
        womenDick = {}

        for ii in range(0, n):

            preference = [-1] * n
            manDick[ii] = [False, 0, preference, 0]
            womenDick[ii] = -1

        return [manDick, womenDick]

    def commitAdultery(manDick, womenDick, woman, husband, sugarBaby):

        manDick[husband][0] = False
        womenDick[woman] = sugarBaby

        return

    def accept(woman, man, manDick, womenDick, preferenceStructure):
        #if there is nothing in the dictionary set it equal to the first man who proposes
        if (womenDick[woman] < 0):
            womenDick[woman] = man
            return True

        husband = womenDick[woman]
        #otherwise, check to see if this women prefers the new man to her current partner
        if (preferenceStructure[man][woman][1] > preferenceStructure[husband][woman][1]):
            
            return False
        
        else:
        #if she does, update the matching
            GS.commitAdultery(manDick, womenDick, woman, husband, man)
            return True

    def fetch(man, preferenceIndex, manDick, preferenceStructure, n):
        #loads man's preference for women into dictionary
        startingIndex = manDick[man][3]

        for woman in range(startingIndex, n):
            
            preference = preferenceStructure[man][woman][0] - 1 #man's preference for current woman
            manDick[man][2][preference] = woman #update man's preferene list
            #if we have found the preference we need
            if preference == preferenceIndex: 
                startingIndex = woman + 1
                break
        #update preference dictionary
        manDick[man][3] = startingIndex
        return

    def propose(man, manDick, womenDick, preferenceStructure, n):
        #if the man is already matched we don't need to do anything
        if (manDick[man][0]):
            return True
        
        matched = False
        preferenceIndex = manDick[man][1]

        while(not matched):
            #if we have not loaded the women corresponding to the preferenceIndex, get it from the preference structure
            if (manDick[man][2][preferenceIndex] < 0):

                GS.fetch(man, preferenceIndex, manDick, preferenceStructure, n)
            #initalize the current women man hopes to propose to
            women = manDick[man][2][preferenceIndex]
            #if she accepts, we are done
            if(GS.accept(women, man, manDick, womenDick, preferenceStructure)):

                matched = True
            #always increment the preference index so we are ready for next iteration
            preferenceIndex += 1
        #reflect changes in dictionary
        manDick[man][0] = True
        manDick[man][1] = preferenceIndex
        return False

    def gs(preferenceStructure, n):

        stable = False
        [manDick, womenDick] = GS.initGS(n)

        while (not stable):
            
            stable = True

            for man in manDick:

                if (not GS.propose(man, manDick, womenDick, preferenceStructure, n)):

                    stable = False

        return womenDick
