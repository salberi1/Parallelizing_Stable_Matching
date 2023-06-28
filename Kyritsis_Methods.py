
FILE_NAME = "init.txt"

def getInit(n):
    #read the preference list and initional matching from FILE_NAME
    file = open(FILE_NAME, "r");
    #define graph
    g = [[[0 for k in range(4)] for j in range(n)] for i in range(n)]
    #assign left values from input data
    for ii in range(0, n):

        nums = file.readline().split()

        for jj in range(0, n):

            g[ii][jj][0] = int(nums[jj])
    #assign right values from input data
    for jj in range(0, n):

        nums = file.readline().split

        for ii in range(0, n):

            g[ii][jj][1] = int(nums[ii])
    #clean-up
    file.close()
    return g

def printGraph(g, n, fileName):
    #prints out the graph to a new file with a specefied name
    file = open(fileName, "x")

    for ii in range(0, n):
        for jj in range(0, n):
            for kk in range(0, 4)
                file.write(g[ii][jj][kk] + " ")
            file.write("|")
        file.write("\n")
    #clean-up
    file.close()
    return





    