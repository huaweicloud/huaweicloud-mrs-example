@echo off

set base=%cd%
cd..
set JAVA=%JAVA_HOME%\bin\java 
set LIBS=%cd%\lib

set CLASS_PATH=%LIBS%\*

set /p input=input the source directory please:
set /p output=input the target directory please:

JAVA -classpath %CLASS_PATH% org.apache.storm.om.StormJarTool "%input%" "%output%"
@pause


:addcp
set CLASS_PATH=%CLASS_PATH%;%1