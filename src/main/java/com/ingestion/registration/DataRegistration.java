package com.ingestion.registration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.ingestion.util.DBUtils;
import com.ingestion.util.HDFSUtils;

public class DataRegistration {
	Connection con = null;
	
	public void execute(String feedId)
			throws IOException, URISyntaxException, SQLException, ClassNotFoundException {
		con 			= 	new DBUtils().connectToDB();
		String query 			= 	"SELECT FEED_ID,SCHEMA_ID FROM FEED WHERE FEED_NAME=?";
		PreparedStatement pStmt = 	con.prepareStatement(query);
		pStmt.setString(1, feedId);
		ResultSet rs 			= 	pStmt.executeQuery();
		String schema_id 		= 	"";
		while (rs.next()) {
			schema_id 		= 	rs.getString(2);
		}
		pStmt.close();
		Boolean toMakeNewEntry = Boolean.FALSE;
		
		String raw_schema_loc = "";
		if (schema_id != "" && feedId != "") {
			raw_schema_loc = DBUtils.getSchemaInfo(con, schema_id);
			if (raw_schema_loc != "") {
				try {
					String lzLocation		=	"/users/anz/landing/schema/newSchema1.txt";
					Boolean schemaMatch 	=	validateSchema(raw_schema_loc, lzLocation, schema_id);
					
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		} else {
			//Schema isn't present, new schema. Go ahead and add it to schema table
			toMakeNewEntry = Boolean.TRUE;
			String srcF  = "/users/anz/landing/schema/newSchema.txt";
			String destF  = "/users/anz/raw/schema/newSchema.txt";
			
			HDFSUtils.copyFileLZToRaw(srcF, destF);
			System.out.println("File has been copied to RAW");
			
			int max_schema_id = DBUtils.fetchMaxSchemaId(con);
			
			DBUtils.insertNewShcema(con, destF, (max_schema_id+1), 1);
			
			DBUtils.insertNewFeed(con, feedId, (max_schema_id+1));
		}
		//This will create new schema
		if(toMakeNewEntry){

		}
	}

	public Boolean validateSchema(String rLocation, String lzLocation, String schema_id) throws IOException, URISyntaxException, NoSuchAlgorithmException, SQLException, ClassNotFoundException {
		Boolean isSchemaMatch	=	Boolean.TRUE;
		Map<String, String> rawSchemaMap	=	HDFSUtils.readHDFSFile(new Path(rLocation));
		Map<String, String> lzSchemaMap		=	HDFSUtils.readHDFSFile(new Path(lzLocation));
		    
				//compare both hashmaps
				if(!lzSchemaMap.isEmpty() && !rawSchemaMap.isEmpty()){
					ArrayList<String> lzKeyLst = new ArrayList<String>(lzSchemaMap.keySet());
					System.out.println(lzKeyLst);
					ArrayList<String> rawKeyLst = new ArrayList<String>(rawSchemaMap.keySet());
					System.out.println(rawKeyLst);
					//check for size of the columns, if matches then check for the order of the elements
					if(lzKeyLst.size() == rawKeyLst.size()){
						for(int i=0; i<rawKeyLst.size(); i++){
							
							if(lzKeyLst.get(i).equals(rawKeyLst.get(i))){
								//order matched
								if(lzSchemaMap.get(lzKeyLst.get(i)).equals(rawSchemaMap.get(rawKeyLst.get(i)))){
									//datatype matched
									System.out.println("schema matches by order, datatypes");
									isSchemaMatch	=	Boolean.TRUE;
								}
								else{
									//datatype changed TODO
									isSchemaMatch	=	Boolean.FALSE;
									System.out.println("Number of columns same, but the Order has changed, creating new version for schema");
								}
							}
							else {
								//Column name changed TODO
								System.out.println("Number of columns same, but the datatype has changed, creating new version for schema");
								isSchemaMatch	=	Boolean.FALSE;
							}
						}
					}
					//Some new columns have been added to schema TODO
					else if(lzKeyLst.size() > rawKeyLst.size()){
						System.out.println("new columns added, creating new version for schema");
						isSchemaMatch	=	Boolean.FALSE;
						DBUtils.insertNewVersionForAShcema(con, lzLocation, Integer.valueOf(schema_id));
					}
					//Some columns hav been deleted
					else if(lzKeyLst.size() < rawKeyLst.size()){
						System.out.println("some columns removed, creating new version for schema");
						isSchemaMatch	=	Boolean.FALSE;
						DBUtils.insertNewVersionForAShcema(con, lzLocation, Integer.valueOf(schema_id));
					}
					if(!isSchemaMatch){
						DBUtils.insertNewVersionForAShcema(con, lzLocation, Integer.valueOf(schema_id));
					}
				}
		return isSchemaMatch;
	}
	
	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, SQLException {
		DataRegistration dr = new DataRegistration();
		
		dr.execute("feed1");
	}
}
