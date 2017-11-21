package com.skm.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hsqldb.Server;

public class MainClass {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		//the entry point will have some info such as 
		//username,linkedin-profile,company name
		//create table for users in hsqldb for containing the id info
		setUpUserDB();
		int consumerNum = 5;
		
		String query = "SELECT id from (SELECT id,COUNT(*) as current_count FROM results GROUP BY id)"
				+ " WHERE current_count=" + consumerNum;
		while(true) {
			Connection con = getConnection();
			ResultSet resultSet = con.prepareStatement(query).executeQuery();
			while(resultSet.next()) {
				
			}
			con.close();
			//check for connect and disconnect
		}
	}

	private static Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/usersDb", "SA", "");
	}

	private static void setUpUserDB() throws ClassNotFoundException, SQLException {
		Server server = new Server();
		server.setDatabaseName(0, "usersDb");
		server.setDatabasePath(0, "mem:usersDb");
		server.setPort(9001);
		server.start();
		createTables();
	}

	private static void createTables() throws ClassNotFoundException, SQLException {
		String url="jdbc:hsqldb:hsql://localhost:9001/usersDb";
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
		Connection conn = DriverManager.getConnection(url, "SA", "");
		String tableCreate = "create table users(id INTEGER IDENTITY PRIMARY KEY,"
				+ "name VARCHAR(255),linkedin_profile VARCHAR(255),company VARCHAR(255))";
		conn.prepareStatement(tableCreate).executeUpdate();
		String resultCreate = "create table results(id INTEGER,type VARCHAR(255),"
				+ "score DOUBLE)";
		conn.prepareStatement(resultCreate).executeUpdate();
	}
	
}
