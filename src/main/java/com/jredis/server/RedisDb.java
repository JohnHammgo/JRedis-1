package com.jredis.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.jredis.utilities.CustomHashMap;
import com.jredis.utilities.DbMap;

public class RedisDb {

	final static Logger log = Logger.getLogger(RedisDb.class);
	
	private String dbPath;
	private CustomHashMap<String, Object> custhashmap;
	private HashMap<String, Object> hashmap;
	private DbMap dbmap;
	
	private RedisDb(String path){
		dbPath = path;
	}
	public static RedisDb newInstance(String path){
		return new RedisDb(path);
	}	
	public CustomHashMap<String, Object> getCustHashMap(){
		return custhashmap;
	}
	public HashMap<String, Object> getHashMap(){
		return hashmap;
	}
	
	public void getDbFromDisk() throws IOException, ClassNotFoundException{
		File dbFile = new File(dbPath);
		if(dbFile.createNewFile() || dbFile.length() == 0){
			//set both maps to empty
			custhashmap = new CustomHashMap<String, Object>();
			hashmap = new HashMap<String, Object>();
			log.debug("Db file does not exist or empty. So new one created");
		}else{
			//get maplist object
			FileInputStream fileIn = new FileInputStream(dbFile);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			dbmap = (DbMap) in.readObject();
			
			//extract maps and allocate
			custhashmap = dbmap.getCHm();
			hashmap = dbmap.getHm();
			in.close();
			fileIn.close();
			log.debug("Db fetched from file");
		}
	}
	public void setDbToDisk() throws IOException{
		
		dbmap = new DbMap();
		dbmap.setCHm(custhashmap);
		dbmap.setHm(hashmap);
		
		FileOutputStream fileOut = new FileOutputStream(dbPath);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(dbmap);
		
	}
	
}
