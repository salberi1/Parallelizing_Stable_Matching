class Detector:

    def __init__(self):
        #initialization
        self.matching = {}
    
    def updateMatching(self, matching):

        self.matching = matching

    def getMatching(self):

        return self.matching
    
    def doStuff(self, matching):

        if self.matching == matching:
            return true

        else:
            return False



m1 = {0:0, 1:1}
m2 = {0:1, 1:1}
m3 = {0:0, 1:0}

detector = Detector()
print("This is the matching upon initialization: " + str(detector.matching))
detector.updateMatching(m1)
print("This is the matching after the update:" + str(detector.matching))
detector.updateMatching(m2)
print("This is the  matching after the second update: " + str(detector.matching))
print(detector.doStuff(m3))
