package com.jredis.utilities;

import java.io.Serializable;
import java.util.HashMap;

import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;

public class DbMap implements Serializable {

	private CustomHashMap<String, Object> custhashmap;
	private HashMap<String, Object> hashmap;
	
	public HashMap<String,Object> getHm(){
		return hashmap;
	}
	public CustomHashMap<String,Object> getCHm(){
		return custhashmap;
	}
	public void setHm(HashMap<String, Object> hm){
		hashmap = hm;
	}
	public void setCHm(CustomHashMap<String, Object> chm){
		custhashmap = chm;
	}
}

