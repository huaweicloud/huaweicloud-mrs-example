说明：Tableau访问HetuEngine，Tableau没有提供相应connector，需要根据Tableau官网指导打包HetuEngine connector。


步骤1：请参考官网“Before you begin”章节准备编译打包环境。其中HetuEngine connector源码保存在tableau_trino_jdbc_hetu_connector目录下。
https://tableau.github.io/connector-plugin-sdk/docs/package-sign#before-you-begin

步骤2：请参考官网“Package the connector”章节编译打包HetuEngine connector，生成.TACO后缀的文件。
https://tableau.github.io/connector-plugin-sdk/docs/package-sign#package-the-connector
