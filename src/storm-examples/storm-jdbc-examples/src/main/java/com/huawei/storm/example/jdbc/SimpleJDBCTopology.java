package com.huawei.storm.example.jdbc;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SimpleJDBCTopology
{    
    private static final String WORD_SPOUT = "WORD_SPOUT";
    
    private static final String JDBC_INSERT_BOLT = "JDBC_INSERT_BOLT";
    
    private static final String JDBC_LOOKUP_BOLT = "JDBC_LOOKUP_BOLT";
    
    //JDBC远端服务器地址
    private static final String JDBC_SERVER_NAME = "请配置JDBC服务端IP地址";
    
    //JDBC服务器端口
    private static final String JDBC_PORT_NUM = "请配置JDBC服务端端口";
    
    //登陆JDBC需要使用的用户名
    private static final String JDBC_USER_NAME = "请配置JDBC登录用户名";
    
    //登陆JDBC需要使用的密码
    private static final String JDBC_PASSWORD = "请配置JDBC登录用户密码";
    
    //datebase表名
    private static final String JDBC_BASE_TBL = "请配置database表名";
    
    //用户创建的源表表名，可自行修改
    private static final String JDBC_ORIGIN_TBL = "ORIGINAL";
    
    //用户创建的目标表表名，可自行修改
    private static final String JDBC_INSERT_TBL = "GOAL";
    
    @SuppressWarnings ("rawtypes")
    public static void main(String[] args) throws Exception
    {
        //connectionProvider配置
        Map<String, Object> hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "org.apache.derby.jdbc.ClientDataSource");
        hikariConfigMap.put("dataSource.serverName", JDBC_SERVER_NAME);
        hikariConfigMap.put("dataSource.portNumber", JDBC_PORT_NUM);
        hikariConfigMap.put("dataSource.databaseName", JDBC_BASE_TBL);
		//derby 示例example数据库不需要配置用户名密码
//        hikariConfigMap.put("dataSource.user", JDBC_USER_NAME);
//        hikariConfigMap.put("dataSource.password", JDBC_PASSWORD);
        hikariConfigMap.put("connectionTestQuery", "select COUNT from " + JDBC_INSERT_TBL);
        
        Config conf = new Config();
        
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
     
       
        //JdbcLookupBolt 实例化
        Fields outputFields = new Fields("WORD", "COUNT");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("WORD", Types.VARCHAR));
        SimpleJdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        String selectSql = "select COUNT from " + JDBC_ORIGIN_TBL + " where WORD = ?";
        JdbcLookupBolt wordLookupBolt = new JdbcLookupBolt(connectionProvider, selectSql, jdbcLookupMapper);
        
        
        //JdbcInsertBolt 实例化
        String tableName = JDBC_INSERT_TBL;
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        JdbcInsertBolt wordInsertBolt =
                new JdbcInsertBolt(connectionProvider, simpleJdbcMapper).withTableName(JDBC_INSERT_TBL).withQueryTimeoutSecs(30);
        
        JDBCSpout wordSpout = new JDBCSpout();
        
        //构造拓扑，wordSpout==>wordLookupBolt==>wordInsertBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(WORD_SPOUT, wordSpout);
        builder.setBolt(JDBC_LOOKUP_BOLT, wordLookupBolt, 1).fieldsGrouping(WORD_SPOUT,new Fields("WORD"));
        builder.setBolt(JDBC_INSERT_BOLT, wordInsertBolt, 1).fieldsGrouping(JDBC_LOOKUP_BOLT,new Fields("WORD"));
        
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}