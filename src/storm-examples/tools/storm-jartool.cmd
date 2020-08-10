@echo off

set base=%cd%
cd..
set JAVA=%JAVA_HOME%\bin\java 
set LIBS=%cd%\lib

set CLASS_PATH=%cd%\src\main\resources
for /r %LIBS% %%b in (*.jar) do call :addcp %%~sb

set /p input=input the source directory please:
set /p output=input the target directory please:

JAVA -classpath %CLASS_PATH% org.apache.storm.om.StormJarTool "%input%" "%output%"
@pause


:addcp
set CLASS_PATH=%CLASS_PATH%;%1