import datetime
import subprocess
import sys


def newFile(fileName):
    outp = open(fileName, 'w+')
    outp.close()
    return


def addlinetoFile(fileName, line):
    outp = open(fileName, 'a+')
    outp.write(line + '\n')
    outp.close()
    return


def addlinetoFilewithnoN(fileName, line):
    outp = open(fileName, 'a+')
    outp.write(line)
    outp.close()
    return


def addlinetoFilewithTime(fileName, line):

    outp = open(fileName, 'a+')
    line = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '  ' + line
    outp.write(line + '\n')
    outp.close()
    return


def addlinetoFileHead(fileName, line):
    outp = open(fileName, 'r+')
    content = outp.read()
    outp.seek(0, 0)
    outp.write(line + '\n' + content)
    outp.close()
    return


def calTimeDiff(t1, t2):
    r1 = t1.split(' ')[0].strip()
    r2 = t1.split(' ')[1].split(',')[0].strip()
    y1 = int(r1.split('-')[0])
    m1 = int(r1.split('-')[1])
    d1 = int(r1.split('-')[2])
    h1 = int(r2.split(':')[0])
    mn1 = int(r2.split(':')[1])
    s1 = int(r2.split(':')[2])

    v1 = t2.split(' ')[0].strip()
    v2 = t2.split(' ')[1].split(',')[0].strip()
    y2 = int(v1.split('-')[0])
    m2 = int(v1.split('-')[1])
    d2 = int(v1.split('-')[2])
    h2 = int(v2.split(':')[0])
    mn2 = int(v2.split(':')[1])
    s2 = int(v2.split(':')[2])

    bias = (y2 - y1) * 365 * 24 * 3600 + (m2 - m1) * 30 * 24 * 3600 + (d2 - d1) * 24 * 3600 \
+ (h2 - h1) * 3600 + (mn2 - mn1) * 60 + (s2 - s1)

    return bias


def calTimeDiffMs(t1, t2):
    r1 = t1.split('.')[0].split(' ')[0].strip()
    r2 = t1.split('.')[0].split(' ')[1].split(',')[0].strip()
    y1 = int(r1.split('-')[0])
    m1 = int(r1.split('-')[1])
    d1 = int(r1.split('-')[2])
    h1 = int(r2.split(':')[0])
    mn1 = int(r2.split(':')[1])
    s1 = int(r2.split(':')[2])
    ms1 = int(t1.split('.')[1])

    v1 = t2.split('.')[0].split(' ')[0].strip()
    v2 = t2.split('.')[0].split(' ')[1].split(',')[0].strip()

    y2 = int(v1.split('-')[0])
    m2 = int(v1.split('-')[1])
    d2 = int(v1.split('-')[2])
    h2 = int(v2.split(':')[0])
    mn2 = int(v2.split(':')[1])
    s2 = int(v2.split(':')[2])
    ms2 = int(t2.split('.')[1])

    bias = round((y2 - y1) * 365 * 24 * 3600 + (m2 - m1) * 30 * 24 * 3600 + (d2 - d1) * 24 * 3600
+ (h2 - h1) * 3600 + (mn2 - mn1) * 60 + (s2 - s1) + round(float(ms2 - ms1) / 1000000, 2), 2)

    return bias


def makeDir(dirStr):
    tmpCommand = 'rm -r ' + dirStr
    if sys.version_info > (3, 0):
        status, output = subprocess.getstatusoutput(tmpCommand)
    else:
        import commands
        status, output = commands.getstatusoutput(tmpCommand)

    tmpCommand = 'mkdir ' + dirStr
    if sys.version_info > (3, 0):
        status, output = subprocess.getstatusoutput(tmpCommand)
    else:
        status, output = commands.getstatusoutput(tmpCommand)

    return


def getvalueByFile(filename, labelname):
    result = open(filename).readlines()
    find = 0
    for line in result:
        if labelname in line:
            find = 1
            break

    if find == 1:
        retStr = line.split('=', 1)[1].strip()
    else:
        retStr = "null"

    return retStr
