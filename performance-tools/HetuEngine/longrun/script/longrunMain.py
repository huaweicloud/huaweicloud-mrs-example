import time
import os
import shutil
import subprocess
import sys
from sys import argv
from public_data import grunbasepath, gretbasepath, gStaticCfg
from public_func import newFile, addlinetoFilewithnoN, addlinetoFilewithTime
from public_func import addlinetoFileHead, calTimeDiff, makeDir, getvalueByFile


def taskrun(taskFile):
    global gidStr
    global grunDir
    global gresultDir
    global goutputdir
    global gloopNum
    global gmainLog

    tmplist = grunDir.split('/')
    tproject = tmplist[len(tmplist) - 1]

    # get the options from the options.cfg
    cfgFile = grunDir + '/options.cfg'
    tmaxNum = getvalueByFile(cfgFile, "pmaxNum")
    tsqlInterval = getvalueByFile(cfgFile, "sqlInterval")
    twaitInterval = getvalueByFile(cfgFile, "waitInterval")
    tprefix = getvalueByFile(cfgFile, "prefix")
    tgetConcurrent = getvalueByFile(cfgFile, "getconcurrent")

    # Modified on 2019.10.30
    workdir = getvalueByFile(cfgFile, "workdir")
    pydir = getvalueByFile(cfgFile, "pythondir")

    # check the concurrent num
    tmpstr = tgetConcurrent
    tmpMaxnum = int(tmaxNum)

    time.sleep(float(tsqlInterval))

    curnum = 0
    while True:
        if sys.version_info > (3, 0):
            status, result = subprocess.getstatusoutput(tmpstr)
        else:
            import commands
            result = commands.getoutput(tmpstr)

        if len(result.strip()) == 0:
            curnum = 0
        else:
            curnum = len(result.split('\n'))

        if curnum >= tmpMaxnum:
            time.sleep(int(twaitInterval))
        else:
            break

    # start to run the task
    addlinetoFileHead(taskFile, tprefix)

    # get batchname to set in the cmd
    if '.' in taskFile:
        tmpstr = taskFile.split('.')[0]
    else:
        tmpstr = taskFile
    tmplist = tmpstr.split('/')
    taskname = tmplist[len(tmplist) - 1]
    tbatchName = taskname.split('-')[0]

    tlabel = taskname.split("-", 1)[1]

    taskid = goutputdir + '/loop' + str(gloopNum) + '-' + taskname + "-py"
    runcmdstr = "python " + pydir + "/taskret.py " + gidStr + " " + tproject + " " + str(gloopNum) \
+ " " + tbatchName + " " + tlabel + ' 1> ' + taskid + '-1 2>' + taskid + '-2;'

    subprocess.Popen(runcmdstr, shell=True, close_fds=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
stderr=subprocess.PIPE, cwd=workdir)

    return


def taskrunCtl(taskFile):
    global gtmptaskDir
    global gidStr
    global grunDir
    global gresultDir
    global gmainLog

    tasklist = open(taskFile).readlines()

    num = 0
    tmpstr = taskFile.split('/')
    tbatchName = tmpstr[len(tmpstr) - 2]
    findsplit = 0

    for line in tasklist:
        if "---" in line:
            findsplit = 1
            # read the label name
            tlabel = line.split('---')[1].strip()
            i = 0
            tmptaskFile = gtmptaskDir + '/' + tbatchName + '-' + tlabel + '.sql'
            newFile(tmptaskFile)

            # to find all tasks in this label
            while i < 500:
                i += 1
                if num + i == len(tasklist):
                    break
                if "---" in tasklist[num + i]:
                    break

                addlinetoFilewithnoN(tmptaskFile, tasklist[num + i])

            # run the taskfile by the options
            addlinetoFilewithTime(gmainLog, "start to run the task: " + tlabel)
            taskrun(tmptaskFile)

        num += 1

    if findsplit == 0:
        tmpstr = taskFile.split('/')
        tlabel = tmpstr[len(tmpstr) - 1]
        tmptaskFile = gtmptaskDir + '/' + tbatchName + '-' + tlabel
        addlinetoFilewithTime(gmainLog, "start to run the task: " + tmptaskFile)
        shutil.copyfile(taskFile, tmptaskFile)
        if gidStr == "uncertained":
            addlinetoFilewithnoN(tmptaskFile, ";")

        taskrun(tmptaskFile)

    return


def batchrun(tBatchDir):
    global gidStr
    global grunDir
    global gresultDir
    global gloopNum
    global gmainLog
    global goutputdir

    result2 = os.listdir(tBatchDir)
    result2.sort()

    for line2 in result2:
        if os.path.isdir(os.path.join(grunDir, line2)) is True:
            continue

        taskFile = os.path.join(tBatchDir, line2)

        taskrunCtl(taskFile)
    return


def looprun():
    global gidStr
    global gloopNum
    global gmainLog
    global grunDir
    global gbatchName

    gloopNum += 1

    addlinetoFilewithTime(gmainLog, "Loop Num: " + str(gloopNum))

    # get the files in grunDir
    result = os.listdir(grunDir)
    result.sort()

    for line in result:
        if os.path.isdir(os.path.join(grunDir, line)) is False:
            continue

        # Get each batch and to run it
        tBatchDir = os.path.join(grunDir, line)
        gbatchName = line

        addlinetoFilewithTime(gmainLog, "Start to run the batch: " + gbatchName)

        batchrun(tBatchDir)

        addlinetoFilewithTime(gmainLog, "batch: " + gbatchName + " Finished")

    return


def looprunCtl():
    global gloopNum
    global gmainLog

    tmaxLoopnum = int(getvalueByFile(gStaticCfg, "maxloopNum"))
    tperiodLenth = getvalueByFile(gStaticCfg, "periodLenth")

    tperiodmin = int(tperiodLenth) * 60

    tbeginTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    while True:
        if gloopNum >= tmaxLoopnum:
            addlinetoFilewithTime(gmainLog, "current gloopNum:" + str(gloopNum) + ", get to the threshold")
            break

        tendTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        tbiasmin = calTimeDiff(tbeginTime, tendTime) / 60
        if tbiasmin > tperiodmin:
            addlinetoFilewithTime(gmainLog, "Loop period ends, by begintime:" + tbeginTime
+ ", endtime:" + tendTime + ", it lasts minutes of:" + str(tbiasmin))
            break

        looprun()

    tendTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    tbiasmin = calTimeDiff(tbeginTime, tendTime) / 60
    addlinetoFilewithTime(gmainLog, "The whole mission ends, by begintime:" + tbeginTime
+ ", endtime:" + tendTime + ", it lasts minutes of:" + str(tbiasmin))

    return


def longrun():
    global gloopNum
    global gmainLog
    global gtmptaskDir
    global goutputdir
    global gresultDir

    # Create result + id Dir
    makeDir(gresultDir)

    # Create result + id Dir
    retDir = gresultDir + '/' + gidStr
    makeDir(retDir)

    # Create temp task Dir
    gtmptaskDir = gresultDir + '/' + gidStr + '-tmptask'
    makeDir(gtmptaskDir)

    # Create output Dir
    goutputdir = gresultDir + '/' + gidStr + '-output'
    makeDir(goutputdir)

    # create main.log and monitor.log, lp.log to cancel in the future
    gmainLog = retDir + '/main.log'
    newFile(gmainLog)

    tmonitorLog = retDir + '/monitor.log'
    newFile(tmonitorLog)

    # read the current timestamp to record
    addlinetoFilewithTime(gmainLog, "Launch to run the program...")
    gloopNum = 0

    # start loop for long running
    looprunCtl()

    return


if __name__ == "__main__":

    global gidStr
    global grunDir
    global gresultDir

    gidStr = 'HetuEngine'
    grunDir = grunbasepath + argv[1]
    gresultDir = gretbasepath + argv[1]

    longrun()

    print("Running Finished!")
