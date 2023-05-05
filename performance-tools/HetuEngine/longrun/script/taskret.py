import subprocess
import sys
import datetime
import os
from sys import argv
from public_data import grunbasepath, gretbasepath, fullSuccess, partSuccess, noSuccess
from public_func import addlinetoFile, addlinetoFilewithTime, calTimeDiffMs, getvalueByFile
from costbias import calcost


def getreturnbyPublic(outputFile1, outputFile2, startTime, num, costsec):
    global gmainLog
    global gidStr
    global gprojectStr

    cfgFile = grunbasepath + gprojectStr + '/options.cfg'
    biasRef = float(getvalueByFile(cfgFile, "biasThreshold"))

    tmpstr = getvalueByFile(cfgFile, "ignoreLimit")
    if tmpstr == 'null':
        tignoreLimit = 0.001
    else:
        tignoreLimit = float(tmpstr)

    secVal = ''
    errFlag = 0
    biasstr = ""

    returnMsg = ''

    #get project, batchname, label
    tmplist = outputFile1.split("/")
    outputfilename = tmplist[len(tmplist) - 1]
    tbatchname = outputfilename.split("-", 2)[1]
    tmpstr = outputfilename.split("-", 2)[2]
    tlabel = tmpstr[0:len(tmpstr) - 2]

    #to check if error
    outputList2 = open(outputFile2).readlines()
    for line in outputList2:
        if line.find('Error') != -1 or line.find('ERROR') != -1 or line.find('error') != -1 \
or line.find('Exception') != -1 or line.find('failed') != -1 or line.find('Failed') != -1:
            errMsg = line[0:40]

            returnMsg = startTime + noSuccess + errMsg
            errFlag = 1
            break

    if errFlag == 0:
        secVal = costsec
        tbias = calcost(gidStr, gprojectStr, tbatchname, tlabel, float(secVal))
        percentbias = "%.1f%%" % (tbias * 100)

        if tbias > biasRef:
            if float(secVal) > tignoreLimit:
                retstr = partSuccess
            else:
                retstr = fullSuccess
            biasstr = "ref bias: " + str(percentbias)
        elif tbias == -1:
            retstr = fullSuccess
            biasstr = ""
        else:
            retstr = fullSuccess
            biasstr = "ref bias: " + str(percentbias)

        returnMsg = startTime + retstr + str(secVal) + " " + biasstr

    return returnMsg


def taskresult(tbatchName, taskname):
    global gidStr
    global grunDir
    global gresultDir
    global gbatchDir
    global gmainLog
    global gtmptaskDir
    global goutputdir

    print("current dir: " + os.getcwd())

    gresultDir = gretbasepath + gprojectStr
    gmainLog = gresultDir + '/' + gidStr + '/main.log'
    gtmptaskDir = gresultDir + '/' + gidStr + '-tmptask'
    goutputdir = gresultDir + '/' + gidStr + '-output'
    tlabel = taskname

    #to get cmd template
    grunDir = grunbasepath + gprojectStr
    cfgFile = grunDir + '/options.cfg'
    runcmdstr = "whoami  1> stdout.log  2> stderr.log"
    if sys.version_info > (3, 0):
        status, result = subprocess.getstatusoutput(runcmdstr)
    else:
        import commands
        result = commands.getoutput(runcmdstr)

    truncmd = getvalueByFile(cfgFile, "runcmd")

    # get runing tasks number
    tgetConcurrent = getvalueByFile(cfgFile, "getconcurrent")

    if sys.version_info > (3, 0):
        status, result = subprocess.getstatusoutput(tgetConcurrent)
    else:
        result = commands.getoutput(tgetConcurrent)
    if len(result.strip()) == 0:
        curnum = 0
    else:
        curnum = len(result.split('\n'))

    #to run the task
    taskid = goutputdir + '/loop' + str(gloopNum) + '-' + tbatchName + '-' + taskname
    preconditionStr = getvalueByFile(cfgFile, "preconditionStr")
    taskFile = gtmptaskDir + '/' + tbatchName + '-' + tlabel + '.sql'
    runcmdstr = preconditionStr + truncmd.replace('REPLACESTR', tbatchName) + ' ' \
+ taskFile + ' 1> ' + taskid + '-1 2>' + taskid + '-2'

    print("runcmdstr: " + runcmdstr)

    startTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    if sys.version_info > (3, 0):
        status, result = subprocess.getstatusoutput(runcmdstr)
    else:
        result = commands.getoutput(runcmdstr)

    endTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    costsec = calTimeDiffMs(startTime, endTime)

    # record the concurrent threads after the task
    if sys.version_info > (3, 0):
        status, result = subprocess.getstatusoutput(tgetConcurrent)
    else:
        result = commands.getoutput(tgetConcurrent)
    if len(result.strip()) == 0:
        curnum = 0
    else:
        curnum = len(result.split('\n'))

    # record the result to monitor.log
    tmonitorLog = gresultDir + '/' + gidStr + '/monitor.log'
    tmptaskFile = gtmptaskDir + '/' + tbatchName + '-' + tlabel + '.sql'
    queryList = open(tmptaskFile).read().strip('\n').strip('\r').split(';')
    num = 0
    for eachQuery in queryList:
        line = eachQuery.strip('\n').strip('\r').strip()
        if len(line) != 0:
            num += 1

    outputFile1 = taskid + '-1'
    outputFile2 = taskid + '-2'

    returnMsg = getreturnbyPublic(outputFile1, outputFile2, startTime[5:19], num, costsec)
    returnList = returnMsg.split('|')
    print("returnMsg:" + returnMsg)

    # write to the monitor log
    lineStr = "%-4s%-14s%-30.30s%-18s%-8s%-4s%-.30s" % (gloopNum, tbatchName, tlabel, returnList[0],
returnList[1], str(curnum + 1), returnList[2])
    addlinetoFile(tmonitorLog, lineStr)
    addlinetoFilewithTime(gmainLog, "batch: " + tbatchName + ", task:" + taskname + " finished")

    return


if __name__ == "__main__":
    global gidStr
    global gprojectStr
    global gloopNum

    gidStr = argv[1]
    gprojectStr = argv[2]
    gloopNum = argv[3]
    tbatchName = argv[4]
    taskname = argv[5]
    taskresult(tbatchName, taskname)

    print("Running Finished!")
