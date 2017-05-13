package com.jredis.server;

import org.apache.log4j.Logger;

public class RedisCommand {

	private String name;
	private int arity;
	private String[] args;
	private String key;
	
	final static Logger log = Logger.getLogger(RedisCommand.class);
	
	/***********
	 * SETTERS AND GETTERS
	 */
	public static RedisCommand newInstance(){
		return new RedisCommand();
	}
	public void setName(String name){
		this.name = name;
	}
	public void setArity(int arity){
		this.arity = arity;
	}
	public void setArgs(String[] args){
		this.args = args;
	}
	
	public String getName(){
		return name;
	}
	public int getArity(){
		return arity;
	}
	public String[] getArgs(){
		return args;
	}
	public void populate(String command, String[] args){
		setName(command);
        setArgs(args);
        log.debug("RedisCommand object populated with command and args");
	}
}
