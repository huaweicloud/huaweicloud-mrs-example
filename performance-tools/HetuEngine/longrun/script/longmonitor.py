import time
from sys import argv
from public_data import grunbasepath, gretbasepath, gStaticCfg
from public_func import getvalueByFile


def monitorCtl():
    global gidStr
    global grunDir
    global gresultDir

    tmonitorLog = gresultDir.strip() + '/' + gidStr + '/monitor.log'
    cfgFile = grunDir.strip() + "/options.cfg"

    twaitInterval = getvalueByFile(cfgFile, "waitInterval")
    tloopLenth = int(getvalueByFile(gStaticCfg, "loopLenth"))
    printedNum = 0
    waittime = 0

    # to print the monitor by color mode
    while True:
        # read the monitor file
        tasklist = open(tmonitorLog).readlines()

        num = 0
        for line in tasklist:
            if len(line.strip()) == 0:
                continue

            num += 1
            if num <= printedNum:
                continue

            # sparse each line
            lineList = line.split()
            retStr = lineList[5].strip()

            if retStr == 'OK':
                retcolorStr = "\033[0;32m "
                errorflag = 0
            elif retStr == 'POK':
                retcolorStr = "\033[0;35m "
                errorflag = 0
            else:
                retcolorStr = "\033[0;31m "
                errorflag = 1

            itemlen = len(lineList)

            okmsg = ''
            retmsg = ''

            if errorflag == 0:
                coststr = '%.02f' % float(lineList[7])
                if itemlen > 8:
                    okmsg = line.split(lineList[7])[1].strip()
            else:
                if itemlen > 7:
                    procnumStr = line.split(lineList[5])[1].strip().split(' ', 1)[0].strip()
                    retmsg = line.split(lineList[5])[1].strip().split(' ', 1)[1].strip()

            normalColorStr = "\033[0;36m "
            timestr = lineList[3] + ' ' + lineList[4]

            if errorflag == 0:
                printstr = "%s%-5s%-14s%-32.30s%-18s%s%-6s%-4s%7s  %-12s" % \
(normalColorStr, lineList[0], lineList[1], lineList[2], timestr, retcolorStr, lineList[5], lineList[6], coststr, okmsg)
            else:
                printstr = "%s%-5s%-14s%-32.30s%-18s%s%-6s%-4s%-.26s" % \
(normalColorStr, lineList[0], lineList[1], lineList[2], timestr, retcolorStr, lineList[5], procnumStr, retmsg)

            print(printstr)
            printedNum += 1

        time.sleep(int(twaitInterval))
        waittime += int(twaitInterval)

        if waittime >= int(tloopLenth / 2) * 60 * 60:
            break

    return


if __name__ == "__main__":

    global gidStr
    global grunDir
    global gresultDir

    gidStr = argv[1]
    grunDir = grunbasepath + argv[2]
    gresultDir = gretbasepath + argv[2]

    monitorCtl()

    print("Monitor Finished!")
