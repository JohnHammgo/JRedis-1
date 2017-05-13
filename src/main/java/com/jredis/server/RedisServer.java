package com.jredis.server;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;

import org.apache.log4j.Logger;

import com.jredis.utilities.Client;
import com.jredis.utilities.Operation;
import com.jredis.utilities.ServerConstants;
import com.jredis.utilities.SharedObjs;

public class RedisServer {
	
	final static Logger log = Logger.getLogger(RedisServer.class);
	
	private int port;
	public static Selector selector;
	private static RedisDb db;
	private ServerSocketChannel serverSock;
	private ArrayList<RedisCommand> redisCommandTable;
	private Queue<Operation> q;
	//Redis commands here
	//lookup query from client and find errors like - unknown command , wrong no
	//of arguments
	//shared objects, integers here
	//load db
	//call corresponding operation
	//receive reply, send to networking class
	/**********
	 *  SETTERS AND GETTERS
	 *********/
	public RedisServer(){
		initServer();
		log.debug("initial server setup");
		initCommandTable();
		log.debug("Command table setup");
	}
	
	public void setDb(RedisDb rdb){
		db = rdb;
	}
	public void setPort(int port){
		this.port = port;
	}
	public void setSelector() throws IOException{
		this.selector = Selector.open();
	}
	public void setServSock() throws IOException{
		this.serverSock = ServerSocketChannel.open();
		serverSock.configureBlocking(false);
	}
		
	public static RedisDb getDb(){
		return db;
	}
	public int getPort(){
		return port;
	}
	public Selector getSelector(){
		return selector;
	}
	public ServerSocketChannel getServSock(){
		return serverSock;
	}
	
	public static RedisServer newInstance(){
		return new RedisServer();
	}
	/********
	 * INIT CONFIGURATIONS
	 ********/
	public void initServer(){
		
		//load shared objects to memory
		SharedObjs.create();
		
	}
	public void initCommandTable(){
			
		redisCommandTable = new ArrayList<RedisCommand>();
		RedisCommand cmd1 = RedisCommand.newInstance();
		cmd1.setName(ServerConstants.GET);
		cmd1.setArity(2);
		redisCommandTable.add(cmd1);
		log.debug("redisComdTable get[0]'s arity is : "+redisCommandTable.get(0).getArity());
		
		RedisCommand cmd2 = RedisCommand.newInstance();
		cmd2.setName(ServerConstants.SET);
		cmd2.setArity(3);
		redisCommandTable.add(cmd2);
		log.debug("redisComdTable get[1]'s arity is : "+redisCommandTable.get(1).getArity());
		
		RedisCommand cmd3 = RedisCommand.newInstance();
		cmd3.setName(ServerConstants.GETBIT);
		cmd3.setArity(3);
		redisCommandTable.add(cmd3);
		log.debug("redisComdTable get[2]'s arity is : "+redisCommandTable.get(2).getArity());
		
		RedisCommand cmd4 = RedisCommand.newInstance();
		cmd4.setName(ServerConstants.SETBIT);
		cmd4.setArity(4);
		redisCommandTable.add(cmd4);
		log.debug("redisComdTable get[3]'s arity is : "+redisCommandTable.get(3).getArity());
		
		RedisCommand cmd5 = RedisCommand.newInstance();
		cmd5.setName(ServerConstants.ZADD);
		cmd5.setArity(4);
		redisCommandTable.add(cmd5);
		log.debug("redisComdTable get[4]'s arity is : "+redisCommandTable.get(4).getArity());
		
		RedisCommand cmd6 = RedisCommand.newInstance();
		cmd6.setName(ServerConstants.ZRANGE);
		cmd6.setArity(4);
		redisCommandTable.add(cmd6);
		log.debug("redisComdTable get[5]'s arity is : "+redisCommandTable.get(5).getArity());
		
		RedisCommand cmd7 = RedisCommand.newInstance();
		cmd7.setName(ServerConstants.ZCARD);
		cmd7.setArity(2);
		redisCommandTable.add(cmd7);
		log.debug("redisComdTable get[6]'s arity is : "+redisCommandTable.get(6).getArity());
		
		RedisCommand cmd8 = RedisCommand.newInstance();
		cmd8.setName(ServerConstants.ZCOUNT);
		cmd8.setArity(4);
		redisCommandTable.add(cmd8);
		log.debug("redisComdTable get[7]'s arity is : "+redisCommandTable.get(7).getArity());
		
		RedisCommand cmd9 = RedisCommand.newInstance();
		cmd9.setName(ServerConstants.SAVE);
		cmd9.setArity(1);
		redisCommandTable.add(cmd9);
		log.debug("redisComdTable get[8]'s arity is : "+redisCommandTable.get(8).getArity());
		
		RedisCommand cmd10 = RedisCommand.newInstance();
		cmd10.setName(ServerConstants.QUIT);
		cmd10.setArity(1);
		redisCommandTable.add(cmd10);
		log.debug("redisComdTable get[9]'s arity is : "+redisCommandTable.get(9).getArity());
		log.debug("redisCommandTable : "+redisCommandTable);
				
	}
	private void executeOperation(Client client, RedisCommand cmd) throws IOException{
		Operation operation = Operation.newInstance(client, cmd);
		log.debug("Object of Operation class created");
		operation.start();
	}
	public void processCommand(String request, Client client) throws IOException{
        log.debug("Inside processRequest");
        request = request.trim();
		String[] parsedRequest = request.split("\\s");
        String command = parsedRequest[0].toLowerCase();
        int arity = -1;
        
        log.debug("Command is : "+command);
        //special case for set operation 
        if(command.equalsIgnoreCase(ServerConstants.SET)){
        	parsedRequest = request.split("\\s", 3);
        }
        log.debug("Parsed request is : ");
        for(String prequest : parsedRequest){
        log.debug(prequest + " ");	
        }        
        log.debug("Parsed request length is : "+ parsedRequest.length);
        
        if(command.equalsIgnoreCase(ServerConstants.GET)){
        	arity = redisCommandTable.get(0).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.SET)){
        	arity = redisCommandTable.get(1).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.GETBIT)){
        	arity = redisCommandTable.get(2).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.SETBIT)){
        	arity = redisCommandTable.get(3).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.ZADD)){
        	arity = redisCommandTable.get(4).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.ZRANGE)){
        	arity = redisCommandTable.get(5).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.ZCARD)){
        	arity = redisCommandTable.get(6).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.ZCOUNT)){
        	arity = redisCommandTable.get(7).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.SAVE)){
        	arity = redisCommandTable.get(8).getArity();
        }else if(command.equalsIgnoreCase(ServerConstants.QUIT)){
        	arity = redisCommandTable.get(9).getArity();
        }else{
        	client.setSelectKeyForWrite(selector);
			client.addErrorReply("unknown command '"+ command +"'"+SharedObjs.newLine);
			log.debug("unknown command "+ command);
			return;
        }
        log.debug("Arity is : "+arity);
        if(arity != parsedRequest.length){
        	client.setSelectKeyForWrite(selector);
            client.addErrorReply("wrong number of arguments for "+command+" command\n");
            log.debug("wrong number of arguments for "+"'"+command+"'"+" command");
        }else{        	
        	//get arguments list from parsed request
        	String[] args = Arrays.copyOfRange(parsedRequest, 1, parsedRequest.length);
        	log.debug("args[] is : ");
        	for(int i=0; i< args.length; i++){
        		log.debug(args[i]+" ");
        	}
        	
        	//populate Redis command object
            RedisCommand rcmd = RedisCommand.newInstance();
            rcmd.populate(command, args);
            
            //Create a new operation object and add it to queue
            executeOperation(client, rcmd);
        }
    }	
}
