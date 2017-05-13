package com.jredis.utilities;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import org.apache.log4j.Logger;

import com.jredis.datastructures.String_T;
import com.jredis.datastructures.ZHash_t;
import com.jredis.server.RedisCommand;
import com.jredis.server.RedisDb;
import com.jredis.server.RedisServer;

public class Operation {

	final static Logger log = Logger.getLogger(RedisDb.class);
	
	private Client client;
	private RedisCommand cmd;
	
	public static Operation newInstance(Client client, RedisCommand cmd){
		return new Operation(client, cmd);
	}
	public Operation(Client client, RedisCommand cmd){
		this.client = client;
		this.cmd = cmd;
	}
	public Client getClient(){
		return client;
	}
	public RedisCommand getCmd(){
		return cmd;
	}
	public void setClient(Client client){
		this.client = client;
	}
	public void setCmd(RedisCommand cmd){
		this.cmd = cmd;
	}
	public void start() throws IOException{
		log.debug("Inside start() function "); 
		String commandName = cmd.getName();
		log.debug("Command is : "+ commandName);
		
		if(commandName.equalsIgnoreCase(ServerConstants.GET)){
			String_T.newInstance(client).get(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.SET)){
			String_T.newInstance(client).set(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.GETBIT)){
			String_T.newInstance(client).getBit(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.SETBIT)){
			String_T.newInstance(client).setBit(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.ZADD)){
			ZHash_t.newInstance(client).zadd(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.ZRANGE)){
			ZHash_t.newInstance(client).zrange(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.ZCARD)){
			ZHash_t.newInstance(client).zcard(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.ZCOUNT)){
			ZHash_t.newInstance(client).zcount(client, cmd);
		}else if(commandName.equalsIgnoreCase(ServerConstants.SAVE)){
			client.setReply(SharedObjs.ok);
			RedisServer.getDb().setDbToDisk();
		}else if(commandName.equalsIgnoreCase(ServerConstants.QUIT)){
			client.setReply(SharedObjs.ok);
			client.getChannel().close();
		}
		
	}
}
