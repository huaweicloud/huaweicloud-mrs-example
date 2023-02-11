#!/bin/bash

#####################################################################
#   FUNCTION   : UnderLineLog
#   DESCRIPTION: 关键性日志信息打印
#   CALLS      : 无
#   CALLED BY  :
#   INPUT      : $1:日志类型 $2:日志数据
#   OUTPUT     : 无
#   LOCAL VAR  : 无
#   USE GLOBVAR: 无
#   RETURN     : 无
#   CHANGE DIR : 无
######################################################################
SET_COLOR_SUCCESS="echo -en \\033[1;32m"
SET_COLOR_FAILURE="echo -en \\033[1;31m"
SET_COLOR_WARNING="echo -en \\033[1;33m"
SET_COLOR_NORMAL="echo -en \\033[0;39m"

function UnderLineLog()
{
  TYPE=$1
  MSG=$2
  TYPE=`echo $TYPE | tr '[:lower:]' '[:upper:]'`

  case $TYPE in
    NORMAL)
        echo -e "\n$MSG"
        $SET_COLOR_NORMAL
        ;;
    WARN)
         $SET_COLOR_WARNING
         echo -e "\nWARN: $MSG"
         $SET_COLOR_NORMAL
         ;;
    FAIL)
         $SET_COLOR_FAILURE
         echo -e "\nERROR: $MSG"
         $SET_COLOR_NORMAL
         ;;
    SUCCESS)
       $SET_COLOR_SUCCESS
       echo -e "\nSUCCESS: $MSG"
       $SET_COLOR_NORMAL
       ;;
  *)
    $SET_COLOR_FAILURE
    UnderLineLog "FAIL" "Unknown log type: $TYPE"
    $SET_COLOR_NORMAL
    exit 1
    ;;
 esac
}

######################################################################
#   FUNCTION   : show_help
#   DESCRIPTION: 提供帮助信息
#   CALLS      : 无
#   CALLED BY  :
#   INPUT      :
#   OUTPUT     :
#   LOCAL VAR  : 无
#   USE GLOBVAR: 无
#   RETURN     : 无
#   CHANGE DIR : 无
######################################################################

function show_help()
{
echo -e "-----------------------------------please attention----------------------------------------"
echo -e "the first you need to download GRAPHBASE CLIENT and enter $BIGDATA_CLIENT/GraphBase/examples"
echo -e " make 'graphbase-core-example' jar   "
echo -e ""
echo -e "you need to input the name of class"
echo -e "Examples:"
echo -e "  $0 com.huawei.graphbase.GraphBaseRestExample"

}

######################################################################
#   FUNCTION   : checkInputs
#   DESCRIPTION: 输入参数校验
#   CALLS      : 无
#   CALLED BY  :
#   INPUT      : HDFS数据目录或文件 HDFS映射文件 HDFS读取数据格式
#   OUTPUT     :
#   LOCAL VAR  : 无
#   USE GLOBVAR: 无
#   RETURN     : 无
#   CHANGE DIR : 无
######################################################################
function checkInput()
{
  CLASS_NAME=$1
  if [ -z $CLASS_NAME ];
  then
      echo "ERROR :need to specify 'class name'"
	  show_help
	  exit 1
  fi
}

######################################################################
#   FUNCTION   : checkJavaENV
#   DESCRIPTION: JDK环境检查,JDK已安装，且不低于指定版本(1.8)
#   CALLS      : 无
#   CALLED BY  :
#   INPUT      : 无
#   OUTPUT     : 无
#   LOCAL VAR  : 无
#   USE GLOBVAR: 无
#   RETURN     : 无
#   CHANGE DIR : 无
######################################################################
DEFAULT_JAVA_VERSION=1.8
function CheckJavaENV()
{
 version=$( java -version 2>&1 | awk -F '"' '/version/ {print $2}')
 if is_lt_default_java_version $version $DEFAULT_JAVA_VERSION; then
    UnderLineLog "FAIL" "need java version >= $DEFAULT_JAVA_VERSION, but current version: $version"
    exit 1
 fi

 if [ -z $JAVA_HOME ];
    then
        UnderLineLog "WARN" "Not Found Java Home, Using Java Builted-in System."
 fi
}

## Is less than default java version?
function is_lt_default_java_version() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$DEFAULT_JAVA_VERSION"; }


######################################################################VA_EXE} ${JAVA_OPTS} -classpath ${CLASSPATH}
#   FUNCTION   : Startup
#   DESCRIPTION: 启动
#   CALLS      : 无
#   CALLED BY  :
#   INPUT      : 无
#   OUTPUT     : 无
#   LOCAL VAR  : 无
#   USE GLOBVAR: 无
#   RETURN     : 无
#   CHANGE DIR : 无
######################################################################
function Startup()
{

  if [ "$CLASS_NAME" == "help" -o "$CLASS_NAME" == "-help" -o  "$CLASS_NAME" == "--help" ];
    then
        show_help
        exit 0
  fi
 
  checkInput $CLASS_NAME

  ## java env
  CheckJavaENV
  ## script path
  SCRIPT_PATH=$(cd $(dirname $(readlink -f "${BASH_SOURCE[0]}")) && pwd )
  cd ${SCRIPT_PATH}
  source /etc/profile
  JAVA_EXE=java
  JAVA_OPTS="$JAVA_OPTS -Xms2G -Xmx4G"
  ## jar loading
  for f in ${SCRIPT_PATH}/lib/*.jar;
  do
	  CLASSPATH=${CLASSPATH}:$f
  done
  
  ${JAVA_EXE} ${JAVA_OPTS} -classpath ${CLASSPATH} ${CLASS_NAME}
}
CLASS_NAME=$1
Startup

