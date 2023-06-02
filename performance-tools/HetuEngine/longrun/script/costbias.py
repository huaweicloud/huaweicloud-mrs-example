import os
from sys import argv
from public_data import grunbasepath, gretbasepath
from public_func import addlinetoFilewithTime


# get the bias and check if normal by reffile
def calcost(idstr, tproject, tbatchName, tlabel, tcost):

    global gmainLog

    gmainLog = gretbasepath + tproject + '/' + idstr + '/main.log'
    greffile = grunbasepath + tproject + '/' + tproject + '_cost.ref'

    #To find the corresponding item in ref file
    if os.path.exists(greffile) is False:
        addlinetoFilewithTime(gmainLog, "project: " + tproject + " has not ref file: " + greffile)
        return -1

    itemlist = open(greffile).readlines()

    tbias = -1
    for line in itemlist:
        if tbatchName in line and tlabel in line:
            # get the result and ref cost
            lineList = line.split()
            retStr = lineList[5].strip()
            if retStr == 'OK':
                refcost = float(lineList[7].strip())
                tbias = round((tcost - refcost) / refcost, 2)
                return tbias
            else:
                return tbias

    return tbias


if __name__ == "__main__":
    idstr = argv[1]
    tproject = argv[2]
    tbatchname = argv[3]
    tlabel = argv[4]
    tcost = argv[5]

    gmainLog = gretbasepath + tproject + '/HetuEngine/main.log'

    ret = calcost(idstr, tproject, tbatchname, tlabel, float(tcost))

    print("bias: " + str(ret))

    print("Running Finished!")
