package com.ingestion.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtils {
	

	public  Connection connectToDB() throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/anz", "root", "");

		return con;
	}
	
	public static String getSchemaInfo(Connection con, String schema_id) throws SQLException {
		String raw_schema_loc	=	"";
		String query;
		PreparedStatement pStmt;
		ResultSet rs;
		query 	= 	"SELECT SCHEMA_LOCATION FROM SCHEMA_METADATA WHERE SCHEMA_ID=?";
		pStmt 	= 	con.prepareStatement(query);
		pStmt.setString(1, schema_id);
		rs 		=	 pStmt.executeQuery();
		while (rs.next()) {
			raw_schema_loc = rs.getString(1);
		}
		pStmt.close();
		return raw_schema_loc;
	}

	public static void insertNewShcema(Connection con, String destF, int max_schema_id, int schema_version) throws SQLException {
		String query;
		PreparedStatement pStmt;
		query 	= 	"insert into SCHEMA_METADATA values(?,?,?);";
		pStmt 	= 	con.prepareStatement(query);
		pStmt.setInt(1, max_schema_id);
		pStmt.setString(2, destF);
		pStmt.setInt(3, schema_version);
		pStmt.execute();
		pStmt.close();
	}
	
	public static void insertNewFeed(Connection con, String feedName, int schema_id) throws SQLException {
		String query;
		PreparedStatement pStmt;
		query 	= 	"insert into FEED(FEED_NAME,SCHEMA_ID) values(?,?);";
		pStmt 	= 	con.prepareStatement(query);
		pStmt.setString(1, feedName);
		pStmt.setInt(2, 1);
		pStmt.execute();
		pStmt.close();
	}

	public static int fetchMaxSchemaId(Connection con) throws SQLException {
		String query;
		PreparedStatement pStmt;
		query 	= 	"SELECT max(SCHEMA_ID) FROM SCHEMA_METADATA";
		pStmt 	= 	con.prepareStatement(query);
		int max_schema_id	=	0; 
		ResultSet rs1 = pStmt.executeQuery();
		while (rs1.next()) {
			 max_schema_id 	= 	rs1.getInt(1);
		}
		pStmt.close();
		return max_schema_id;
	}
	
	public static void insertNewVersionForAShcema(Connection con, String lzLocation, int schema_id) throws SQLException, ClassNotFoundException {
		PreparedStatement pStmt;
		String	query 	= 	"SELECT max(SCHEMA_VERSION) FROM SCHEMA_METADATA WHERE SCHEMA_ID = ?";
		pStmt 	= 	con.prepareStatement(query);
		pStmt.setInt(1, (schema_id));
		Integer		max_schema_version	=	0;
		ResultSet rs1 	=	pStmt.executeQuery();
		while (rs1.next()) {
			 max_schema_version 	= 	rs1.getInt(1);
		}
		pStmt.close();
		
		insertNewShcema(con, lzLocation, schema_id, (max_schema_version+1));
	}
}
