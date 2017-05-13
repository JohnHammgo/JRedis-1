package com.jredis.utilities;

public class SharedObjs {

	public static String ok;
	public static String err;
	public static String zero;
	public static String one;
	public static String negOne;
	public static String nullBulk;
	public static String nullMultiBUlk;
	public static String emptyMultiBulk;
	public static String wrongTypeErr;
	public static String syntaxError;
	public static String notIntBit;
	public static String notFloat;
	public static String znotFloat;
	public static String notBit;
	public static String notInt;
	public static String invalidDb;
	public static String loadingDb;
	public static String noDb;
	public static String space;
	public static String colon;
	public static String plus;
	public static String dollar;
	public static String newLine;
	
	//think about shared integers 
	
	public static void create(){
		ok = "+OK\n";
		err = "-ERR";
		zero = ":0\n";
		one = ":1\n";
		negOne = ":-1\n";
		nullBulk = "$-1\n";
		nullMultiBUlk = "*-1\n";
		emptyMultiBulk = "*0\n";
		wrongTypeErr = "-WRONGTYPE Operation against a key holding the wrong kind of value\n";
		syntaxError = "syntax error\n";
		notIntBit = "bit offset is not an integer or out of range\n";
		notInt = "value is not an integer or out of range\n";
		notBit = "bit is not an integer or out of range\n";
		notFloat = "value is not a valid float\n";
		znotFloat = "min or max is not a float\n";
		noDb = "-DB_PATH not entered";
		invalidDb = "-INVALID db extension. Must be .jrdb\n";
		loadingDb = "-LOADING Redis is loading the dataset in memory";
		space = " ";
		colon = ":";
		plus = "+";
		dollar = "$";
		newLine = "\n";
		
	}
}
