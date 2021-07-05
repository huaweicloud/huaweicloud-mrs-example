/*
 * CQL示例工程，从随机数据生成算子中生成随机数据
 * 将处理完毕的数据打印在控制台(一般是日志文件)中
 */

--生成随机数，每秒1条
CREATE INPUT STREAM s
	(id INT, name STRING, type INT)
 SOURCE randomgen
           PROPERTIES
           ( "timeUnit" = "SECONDS", "period" = "1",
           "eventNumPerPeriod" = "1", "isSchedule" = "true");

--将计算结果输出到控制台(日志文件)中
CREATE OUTPUT STREAM rs
(type INT, cc INT)
 SINK ConsoleOutput;
					
--统计每个20秒内产生的id>5的数据按照type分组之后每组数量
--时间跳动窗口，每20秒产生一次结算结果
INSERT INTO STREAM rs SELECT type, count(id) as cc FROM s[RANGE 20 SECONDS BATCH] WHERE id > 5 GROUP BY type;	
--提交应用程序
SUBMIT APPLICATION example;