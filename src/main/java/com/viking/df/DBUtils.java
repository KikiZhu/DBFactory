/*
 * Copyright (C), China, Viking
 * FileName: DBUtils.java
 * Author:   viking
 * Date:     2017年8月20日 上午12:23:11
 * Description: //模块目的、功能描述      
 * History: //修改记录
 * <author>      <time>      <version>    <desc>
 * 修改人姓名             修改时间            版本号                  描述
 */
package com.viking.df;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * 〈一句话功能简述〉数据库打开关闭<br> 
 * 〈功能详细描述〉
 *
 * @author viking
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class DBUtils {
	private static final Logger logger = Logger.getLogger(DBUtils.class);
	/**
	 * 数据库配置文件
	 */
	private static final String PROPERTIES = "src/main/resources/db.properties";
	
	/**
	 * 功能描述: 加载配置获取数据库DataSource<br>
	 * 〈功能详细描述〉
	 *
	 * @return HikariDataSource
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static HikariDataSource getDataSource(){
		HikariConfig config = new HikariConfig(PROPERTIES);
		HikariDataSource ds = new HikariDataSource(config);
		return ds;
	}
	
	/**
	 * 功能描述: 关闭数据库连接<br>
	 * 〈功能详细描述〉释放连接资源
	 *
	 * @param conn
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static void closeConn(Connection conn){
		try {
			conn.close();
		} catch (Exception e) {
			logger.error("Close Connection Fail! "+e.getMessage());
		}
	}
	
	/**
	 * 功能描述: 关闭数据库操作时预加载Statement<br>
	 * 〈功能详细描述〉释放预加载资源Statement
	 *
	 * @param statement
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static void closeStatement(Statement statement){
		try {
			statement.close();
		} catch (Exception e) {
			logger.error("Close Statement Fail! "+e.getMessage());
		}
	}
	
	/**
	 * 功能描述: 关闭数据库操作时预加载PreparedStatement<br>
	 * 〈功能详细描述〉释放预加载资源PreparedStatement
	 *
	 * @param preStatement
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static void closePreparedStatement(PreparedStatement preStatement){
		try {
			preStatement.close();
		} catch (Exception e) {
			logger.error("Close PreparedStatement Fail! "+e.getMessage());
		}
	}
	
	/**
	 * 功能描述: 关闭数据库执行后结果集<br>
	 * 〈功能详细描述〉释放执行后结果
	 *
	 * @param resultSet
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static void closeResultSet(ResultSet resultSet){
		try {
			resultSet.close();
		} catch (Exception e) {
			logger.error("Close ResultSet Fail! "+e.getMessage());
		}
	}
	
	/**
	 * 功能描述: 打开数据库连接<br>
	 * 〈功能详细描述〉通过DataSource获取连接
	 *
	 * @return conn
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	protected static Connection getConn(){
		Connection conn = null;
		try {
			conn = getDataSource().getConnection();
		} catch (Exception e) {
			getDataSource().close();
			logger.error("Get Connection Fail! "+e.getMessage());
		}
		return conn;
	}
	
	/**
	 * 功能描述: 关闭数据库连接<br>
	 * 〈功能详细描述〉
	 *
	 * @param conn
	 * @param statement
	 * @param resultSet
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	protected static void close(Connection conn,Statement statement,ResultSet resultSet){
		if (resultSet != null) {
			closeResultSet(resultSet);
		} 
		if (statement != null) {
			closeStatement(statement);
		}
		if (conn != null) {
			closeConn(conn);
		}
	}
	
	/**
	 * 功能描述: 关闭数据库连接<br>
	 * 〈功能详细描述〉
	 *
	 * @param conn
	 * @param preparedStatement
	 * @param resultSet
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	protected static void close(Connection conn,PreparedStatement preparedStatement,ResultSet resultSet){
		if (resultSet != null) {
			closeResultSet(resultSet);
		} 
		if (preparedStatement != null) {
			closePreparedStatement(preparedStatement);
		}
		if (conn != null) {
			closeConn(conn);
		}
	}
}
