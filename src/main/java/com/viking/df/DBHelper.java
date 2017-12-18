/*
 * Copyright (C), China, Viking
 * FileName: DBHelper.java
 * Author:   viking
 * Date:     2017年8月21日 上午11:10:46
 * Description: //模块目的、功能描述      
 * History: //修改记录
 * <author>      <time>      <version>    <desc>
 * 修改人姓名             修改时间            版本号                  描述
 */
package com.viking.df;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 〈一句话功能简述〉数据库操作类<br>
 * 〈功能详细描述〉对数据库表进行操作
 *
 * @author viking
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class DBHelper {
	private static final Logger logger = Logger.getLogger(DBHelper.class);

	private static Connection conn = null;
	private static PreparedStatement preparedStatement = null;
	private static CallableStatement callableStatement = null;
	
	/**
	 * 功能描述: 关闭数据库<br>
	 * 〈功能详细描述〉关闭数据库
	 *
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static void close() {
		close(null);
	}
	
	/**
	 * 功能描述: 关闭数据库<br>
	 * 〈功能详细描述〉关闭数据集
	 *
	 * @param resultSet
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static void close(ResultSet resultSet) {
		DBUtils.close(conn, preparedStatement, resultSet);
	}
	
	/**
	 * 功能描述: 关闭数据库<br>
	 * 〈功能详细描述〉关闭数据库预加载和数据集
	 *
	 * @param statement
	 * @param resultSet
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static void close(Statement statement,ResultSet resultSet){
		DBUtils.close(conn, statement, resultSet);
	}
	
	/**
	 * 功能描述: 关闭数据库<br>
	 * 〈功能详细描述〉关闭数据库预加载和数据集
	 *
	 * @param preparedStatement
	 * @param resultSet
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static void close(PreparedStatement preparedStatement,ResultSet resultSet){
		DBUtils.close(conn, preparedStatement, resultSet);
	}
	
	/**
	 * 功能描述: 获取PreparedStatement<br>
	 * 〈功能详细描述〉
	 * 
	 * @param sql
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static void getPreparedStatement(String sql) {
		conn = DBUtils.getConn();
		try {
			preparedStatement = conn.prepareStatement(sql);
		} catch (SQLException e) {
			logger.error("Get PreparedStatement Fail : " + e.getMessage());
		}
	}

	/**
	 * 功能描述: 获取CallableStatement<br>
	 * 〈功能详细描述〉
	 * 
	 * @param procedureSql
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static void getCallableStatement(String procedureSql) {
		conn = DBUtils.getConn();
		try {
			callableStatement = conn.prepareCall(procedureSql);
		} catch (SQLException e) {
			logger.error("Get CallableStatement Fail : " + e.getMessage());
		}
	}

	/**
	 * 功能描述: ResultSet转为List<br>
	 * 〈功能详细描述〉
	 * 
	 * @param rs
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	private static List<Object> ResultToListMap(ResultSet rs) {
		List<Object> list = new ArrayList<Object>();
		try {
			while (rs.next()) { 
				ResultSetMetaData md;
				md = rs.getMetaData();
				Map<Object, Object> map = new HashMap<Object, Object>();
				for (int i = 1; i <= md.getColumnCount(); i++) {
					map.put(md.getColumnLabel(i), rs.getObject(i));
				}
				list.add(map);
			}
		} catch (SQLException e) {
			logger.error("Result to ListMap Fail : " + e.getMessage());
		}finally {
			close(rs);
		}
		return list;
	}
	
	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉用于查询，返回结果集
	 * 
	 * @param sql sql语句
	 * @return 结果集
	 * @throws SQLException
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static List<Object> query(String sql) {
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
		} catch (Exception e) {
			logger.error("Query Fail! " + e.getMessage());
		} 
		return ResultToListMap(rs);
	}
	
	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉用于查询，返回结果map
	 * @param sql sql语句
	 * @return 结果map
	 * @throws SQLException
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static List<HashMap<String, String>> queryToMap(String sql) {
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
		} catch (Exception e) {
			logger.error("Query To Map Fail! " + e.getMessage());
		} 
		
		List<HashMap<String,String>> listMap = new ArrayList<HashMap<String,String>>();
		try {
			while (rs.next()) { 
				ResultSetMetaData md;
				md = rs.getMetaData();
				HashMap<String, String> map = new HashMap<String, String>();
				for (int i = 1; i <= md.getColumnCount(); i++) {
					map.put(md.getColumnLabel(i).toString(), rs.getObject(i).toString());
				}
				listMap.add(map);
			}
		} catch (SQLException e) {
			logger.error("Result to ListMap Fail : " + e.getMessage());
		}finally {
			close(rs);
		}
		return listMap;
	}
	
	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉用于查询，返回指定列的结果集
	 * 
	 * @param sql sql语句
	 * @return 结果集
	 * @throws SQLException
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static Map<String, String> query(String sql,String columnKey, String columnValue) {
		ResultSet rs = null;
		Map<String, String> resultMap = new HashMap<String, String>();
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				String key = rs.getString(columnKey);
				String value = rs.getString(columnValue);
				resultMap.put(key, value);
			}
		} catch (Exception e) {
			logger.error("query Fail : " + e.getMessage());
		}finally{
			close(rs);
		}
		return resultMap;
	}
	
	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉用于查询，返回指定列的单个结果
	 * 
	 * @param sql sql语句
	 * @return string
	 * @throws SQLException
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static String query(String sql,String columnName) {
		ResultSet rs = null;
		String columeValue = null;
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				columeValue = rs.getString(columnName);
			}
		} catch (Exception e) {
			logger.error("Query Someone Columnname Fail! " + e.getMessage());
		}finally{
			close(rs);
		}
		return columeValue;
	}

	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉用于带Object类型参数查询，返回结果集
	 * 
	 * @param sql sql语句
	 * @param paramters 参数集合
	 * @return 结果集
	 * @throws SQLException
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static List<Object> query(String sql, Object... paramters) {
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			for (int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			rs = preparedStatement.executeQuery();
		} catch (Exception e) {
			logger.error("Query Object Paramters Fail! " + e.getMessage());
		}
		return ResultToListMap(rs);
	}

	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉查询返回单个结果的值，如count/min/max等
	 * 
	 * @param sql sql语句
	 * @return 结果值
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static Object singleQuery(String sql) {
		Object result = null;
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				result = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("Query Function SQL Fail! " + e.getMessage());
		} finally {
			close(rs);
		}
		return result;
	}

	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉根据参数查询返回单个结果的值，如count/min/max等
	 * 
	 * @param sql sql语句
	 * @param paramters 参数列表
	 * @return 结果值
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static Object singleQuery(String sql, Object... paramters) {
		Object result = null;
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			for (int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			rs = preparedStatement.executeQuery();
			if (rs.next()) {
				result = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("Query Function SQL Fail! " + e.getMessage());
		} finally {
			close(rs);
		}
		return result;
	}

	/**
	 * 功能描述: 执行增删改<br>
	 * 〈功能详细描述〉用于增删改sql语句执行
	 * 
	 * @param sql sql语句
	 * @return 影响行数
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static int update(String sql) {
		int i = -1;
		try {
			getPreparedStatement(sql);
			i = preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Update SQL Fail! " + e.getMessage());
		} finally {
			close();
		}
		return i;
	}

	/**
	 * 功能描述: 执行增删改<br>
	 * 〈功能详细描述〉根据参数用于增删改sql语句执行
	 * 
	 * @param sql sql语句
	 * @param paramters 参数
	 * @return 影响行数
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static int update(String sql, Object... paramters) {
		int j = -1;
		try {
			getPreparedStatement(sql);
			for (int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			j = preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Update Object paramters Fail! " + e.getMessage());
		} finally {
			close();
		}
		return j;
	}

	/**
	 * 功能描述: 执行插入操作<br>
	 * 〈功能详细描述〉插入值后返回主键值
	 * 
	 * @param sql 插入sql语句
	 * @return 返回主键结果
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	@SuppressWarnings("static-access")
	public static Object insertWithReturnPrimeKey(String sql) {
		ResultSet rs = null;
		Object result = null;
		try {
			conn = DBUtils.getConn();
			preparedStatement = conn.prepareStatement(sql, preparedStatement.RETURN_GENERATED_KEYS);
			preparedStatement.execute();
			rs = preparedStatement.getGeneratedKeys();
			if (rs.next()) {
				result = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("Insert SQL And Return Primekey Fail! " + e.getMessage());
		} finally {
			close(rs);
		}
		return result;
	}

	/**
	 * 功能描述: 执行插入操作<br>
	 * 〈功能详细描述〉根据参数执行插入值后返回主键值
	 * 
	 * @param sql
	 * @param paramters
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static Object insertWithReturnPrimeKey(String sql, Object... paramters) {
		ResultSet rs = null;
		Object result = null;
		try {
			conn = DBUtils.getConn();
			preparedStatement = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			for (int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			preparedStatement.execute();
			rs = preparedStatement.getGeneratedKeys();
			if (rs.next()) {
				result = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("Insert SQL With Object paramters And Return Primekey Fail! " + e.getMessage());
		} finally {
			close(rs);
		}
		return result;
	}

	/**
	 * 功能描述: 调用存储过程执行查询<br>
	 * 〈功能详细描述〉
	 * 
	 * @param procedureSql 存储过程
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static List<Object> callableQuery(String procedureSql) {
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);
			rs = callableStatement.executeQuery();
		} catch (Exception e) {
			logger.error("Call ProcedureSQL Fail! " + e.getMessage());
		} 
		return ResultToListMap(rs);
	}

	/**
	 * 功能描述: 执行查询<br>
	 * 〈功能详细描述〉调用存储过程（带参数）,执行查询
	 * 
	 * @param procedureSql 存储过程
	 * @param paramters 参数表
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static List<Object> callableQuery(String procedureSql, Object... paramters) {
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);

			for (int i = 0; i < paramters.length; i++) {
				callableStatement.setObject(i + 1, paramters[i]);
			}
			rs = callableStatement.executeQuery();
		} catch (Exception e) {
			logger.error("Call ProcedureSQL With Object paramters Fail! " + e.getMessage());
		} 
		return ResultToListMap(rs);
	}

	/**
	 * 功能描述: 调用存储过程，查询单个值<br>
	 * 〈功能详细描述〉
	 * 
	 * @param procedureSql
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static Object callableGetSingle(String procedureSql) {
		Object result = null;
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);
			rs = callableStatement.executeQuery();
			while (rs.next()) {
				result = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("Call ProcedureSQL Get Single Value Fail! " + e.getMessage());
		} finally {
			close(rs);
		}
		return result;
	}

	/**
	 * 功能描述: 调用存储过程(带参数)，查询单个值<br>
	 * 〈功能详细描述〉
	 * 
	 * @param procedureSql
	 * @param paramters
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static Object callableGetSingle(String procedureSql, Object... paramters) {
		Object result = null;
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);

			for (int i = 0; i < paramters.length; i++) {
				callableStatement.setObject(i + 1, paramters[i]);
			}
			rs = callableStatement.executeQuery();
			while (rs.next()) {
				result = rs.getObject(1);
			}
		} catch (Exception e) {
			logger.error("Call ProcedureSQL With Object paramters Fail! " + e.getMessage());
		} finally {
			close(rs);
		}
		return result;
	}

	/**
	 * 功能描述: 调用存储过程，执行增删改<br>
	 * 〈功能详细描述〉
	 * 
	 * @param procedureSql 存储过程
	 * @return 影响行数
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static int callableUpdate(String procedureSql) {
		int i = -1;
		try {
			getCallableStatement(procedureSql);
			i = callableStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Call ProcedureSQL To Update Fail! " + e.getMessage());
		} finally {
			close();
		}
		return i;
	}

	/**
	 * 功能描述: 执行增删改<br>
	 * 〈功能详细描述〉调用存储过程（带参数），执行增删改
	 * 
	 * @param procedureSql 存储过程
	 * @param parameters 影响行数
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static int callableUpdate(String procedureSql, Object... parameters) {
		int j = -1;
		try {
			getCallableStatement(procedureSql);
			for (int i = 0; i < parameters.length; i++) {
				callableStatement.setObject(i + 1, parameters[i]);
			}
			j = callableStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Call ProcedureSQL With Object Parameters To Update Fail! " + e.getMessage());
		} finally {
			close();
		}
		return j;
	}

	/**
	 * 功能描述: 批量更新数据<br>
	 * 〈功能详细描述〉
	 * 
	 * @param sqlList
	 * @return
	 * @see [相关类/方法](可选)
	 * @since [产品/模块版本](可选)
	 */
	public static int[] batchUpdate(List<String> sqlList) {

		int[] result = new int[] {};
		Statement statenent = null;
		try {
			conn = DBUtils.getConn();
			conn.setAutoCommit(false);
			statenent = conn.createStatement();
			for (String sql : sqlList) {
				statenent.addBatch(sql);
			}
			result = statenent.executeBatch();
			conn.commit();
		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				logger.error("Batch Update Fail,Rollback Fail! " + e.getMessage());
			}
			logger.error("Batch Update Fail! " + e.getMessage());
		} finally {
			close(statenent, null);
		}
		return result;
	}

}
