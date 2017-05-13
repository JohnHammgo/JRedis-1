package com.jredis.datastructures;

import java.nio.channels.ClosedChannelException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.jredis.server.Networking;
import com.jredis.server.RedisCommand;
import com.jredis.server.RedisServer;
import com.jredis.utilities.Client;
import com.jredis.utilities.CustomHashMap;
import com.jredis.utilities.FLAG;
import com.jredis.utilities.SharedObjs;

public class String_T {

	//deploy get command - check for wrong type err, else return OK
	//add reply and send
	//appropriate map
	final static Logger log = Logger.getLogger(String_T.class);
	private CustomHashMap<String, Object> custhashmap;
	private HashMap<String, Object> hashmap;
	
	public String_T(Client client){
		custhashmap = RedisServer.getDb().getCustHashMap();
		hashmap = RedisServer.getDb().getHashMap();
		log.debug("Custom Hash Map : "+ custhashmap);
		log.debug("Hash Map : "+ hashmap);
	}
	public static String_T newInstance(Client client){
		return new String_T(client);
	}
	
	public void get(Client client, RedisCommand cmd) throws ClosedChannelException{
		
		log.debug("Inside GET command");
		String[] args = cmd.getArgs();		
		String key = args[0];
		log.debug("args[] : "+args+" key: "+key);
		if(hashmap.get(key) != null){
			client.setReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		
		CharSequence val = (CharSequence)custhashmap.get(key);
		log.debug("val : "+val);
		if(val == null){
			client.setReply(SharedObjs.nullBulk);
			log.debug("Value not found");
		}else{
			client.setReply(SharedObjs.dollar+val.length()+SharedObjs.newLine+val+SharedObjs.newLine);
			log.debug("Value fetched : "+ val);
		}	
	}
	
	public void set(Client client, RedisCommand cmd) throws ClosedChannelException{
		log.debug("Inside SET command");
			
		// create a flag class static values
		int flag1 = FLAG.nil;
		int flag2 = FLAG.nil;
		String[] args = cmd.getArgs();
		long msec = (long) Double.POSITIVE_INFINITY;
		long sec = (long) Double.POSITIVE_INFINITY;
		
		String key = args[0];
		log.debug("args[] : "+args+" key :"+key);
		for(int i=0; i<args.length; i++){
			log.debug(args[i]+ " ");
		}
		if(hashmap.get(key) != null){
			client.setReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		String[] optionAndVal = args[1].split("\\s");
		log.debug("optionAndVal size :"+optionAndVal.length);
		for(int i=0; i<optionAndVal.length; i++){
			log.debug(optionAndVal[i]);
		}
		String val = optionAndVal[0];
		log.debug("val : "+val);
		for(int i=1; i<optionAndVal.length; i++){
			log.debug("Inside for loop");
			String arg = optionAndVal[i];
			log.debug("option : "+arg);
			if(arg.equalsIgnoreCase("PX") && (flag1 == FLAG.nil)
					&& optionAndVal[i+1] != null){
				flag1 = FLAG.px;
				msec = Long.parseLong(optionAndVal[i+1]);
				i+=1;
			}
			else if(arg.equalsIgnoreCase("EX") && (flag1 == FLAG.nil)
					&& optionAndVal[i+1] != null){
				flag1 = FLAG.ex;
				sec = Long.parseLong(optionAndVal[i+1]);;
				i+=1;
			}
			else if(arg.equalsIgnoreCase("nx") && (flag2 == FLAG.nil)){
				flag2 = FLAG.nx;
			}
			else if(arg.equalsIgnoreCase("xx") && (flag2 == FLAG.nil)){
				flag2 = FLAG.xx;
			}
			else{
				log.debug(SharedObjs.syntaxError);
				client.addErrorReply(SharedObjs.syntaxError);
	            return;
			}
			
		}
		
		if(optionAndVal.length == 1){
			log.debug("optionless");
			setTime(key, val, null);
			client.setReply(SharedObjs.ok);
			log.debug(SharedObjs.ok);
		}
		if(flag1 == FLAG.px && flag2 == FLAG.nil){
			log.debug("px");
			setTime(key, val, msec);
			client.setReply(SharedObjs.ok);
			log.debug(SharedObjs.ok);
			
		}
		if(flag1 == FLAG.px && flag2 == FLAG.nx){
			log.debug("px nx");
			StringBuilder orgVal = (StringBuilder) custhashmap.get(key);
			if(orgVal == null){
				setTime(key, val, msec);
				log.debug("new value set : (px) (nx) "+ val);
				client.setReply(SharedObjs.ok);
				log.debug(SharedObjs.ok);
			}
			else{
				client.setReply(SharedObjs.nullBulk);
				log.debug(SharedObjs.ok);
			}
		}
		if(flag1 == FLAG.px && flag2 == FLAG.xx){
			log.debug("px xx");
			StringBuilder orgVal = (StringBuilder) custhashmap.get(key);
			log.debug("Org val : "+orgVal);
			if(!(orgVal == null)){
				setTime(key, val, msec);
				log.debug("new value set : (px) (xx) "+ val);
				client.setReply(SharedObjs.ok);
				log.debug(SharedObjs.ok);
			}
			else{
				client.setReply(SharedObjs.nullBulk);
				log.debug(SharedObjs.nullBulk);
			}
		}
		if(flag1 == FLAG.ex && flag2 == FLAG.nil){
			log.debug("ex");
			setTime(key, val, sec*1000);
			client.setReply(SharedObjs.ok);
			log.debug("OK");
		}
		if(flag1 == FLAG.ex && flag2 == FLAG.nx){
			log.debug("ex nx");
			StringBuilder orgVal = (StringBuilder) custhashmap.get(key);
			if(orgVal == null){
				setTime(key, val, sec*1000);
				log.debug("new value set : (ex) (nx) "+ val);
				client.setReply(SharedObjs.ok);
				log.debug(SharedObjs.ok);
			}
			else{
				client.setReply(SharedObjs.nullBulk);
				log.debug("Reply : "+ SharedObjs.nullBulk);
			}
		}
		if(flag1 == FLAG.ex && flag2 == FLAG.xx){
			log.debug("ex xx");
			StringBuilder orgVal = (StringBuilder) custhashmap.get(key);
			log.debug("orgVal : "+orgVal);
			if(!(orgVal == null)){
				setTime(key, val, sec*1000);
				log.debug("new value set : (ex) (xx) "+ val);
				client.setReply(SharedObjs.ok);
				log.debug(SharedObjs.ok);
			}
			else{
				client.addErrorReply(SharedObjs.nullBulk);
				log.debug(SharedObjs.nullBulk);
			}
		}
		if(flag1 == FLAG.nil && flag2 == FLAG.nx){
			log.debug("nx");
			StringBuilder orgVal = (StringBuilder) custhashmap.get(key);
			if(orgVal == null){
				setTime(key, val, null);
				log.debug("new value set : (nx) "+ val);
				client.setReply(SharedObjs.ok);
				log.debug(SharedObjs.ok);
			}
			else{
				client.setReply(SharedObjs.nullBulk);
				log.debug(SharedObjs.nullBulk);
			}
				
		}
		if(flag1 == FLAG.nil && flag2 == FLAG.xx){
			log.debug("xx");
			StringBuilder orgVal = (StringBuilder) custhashmap.get(key);
			log.debug("orgVal : "+orgVal);
			if(!(orgVal == null)){
				setTime(key, val, null);
				log.debug("new value set : (xx) "+ val);
				client.setReply(SharedObjs.ok);
				log.debug("Reply : "+SharedObjs.ok);
			}
			else{
				client.setReply(SharedObjs.nullBulk);
				log.debug("Reply : "+SharedObjs.nullBulk);
			}
				
		}
	}

	public void setTime(String key, String val, Long time){
		log.debug("Inside settime");
		if(time == null)
			custhashmap.put(key, new StringBuilder(val));
		else
			custhashmap.putExpire(key, new StringBuilder(val), time);
		log.debug("Value : "+val+" and key : "+key+" put successfully");
	}
	
	public void getBit(Client client, RedisCommand cmd) throws ClosedChannelException{
		String[] args = cmd.getArgs();
		String key = args[0];
		log.debug("args[] : "+ args+" key : "+key);
		if(hashmap.get(key) != null){
			client.addErrorReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		int pos = -1;
		try{
			pos = Integer.parseInt(args[1]);
			log.debug("pos : "+ pos);
		}catch(NumberFormatException ex){
			client.addErrorReply(SharedObjs.notIntBit);
			log.debug("Reply : "+ SharedObjs.notIntBit);
		}
		StringBuilder str = (StringBuilder) custhashmap.get(key);
		log.debug("str: "+str);
		if(str != null){
			int c = -1;
			try{
				c = str.charAt(pos / 8);
			}catch(StringIndexOutOfBoundsException ex){
				client.setReply(SharedObjs.zero);
				return;
			}
			int val = (c >> (7 - (pos % 8)) & 1) != 0 ? 1 : 0;
			if(val == 0){
				client.setReply(SharedObjs.zero);
				log.debug("Reply : "+SharedObjs.zero);
			}
			else if(val ==1){
				client.setReply(SharedObjs.one);
				log.debug("Reply : "+SharedObjs.zero);
			}
		}
		else{
			client.setReply(SharedObjs.zero);
		}
	}
	
	public void setBit(Client client, RedisCommand cmd) throws ClosedChannelException{
		String[] args = cmd.getArgs();
		String key = args[0];
		log.debug("args [] : "+args+" key: "+key);
		if(hashmap.get(key) != null){
			client.addErrorReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		int pos = -1;
		char c = '\0';
		int orgVal = -1;
		try{
			pos = Integer.parseInt(args[1]);
			log.debug("pos : "+pos);
		}catch(NumberFormatException ex){
			client.addErrorReply(SharedObjs.notIntBit);
			return;
		}
		int bitval = Integer.parseInt(args[2]);
		log.debug("bitval : "+bitval);
		StringBuilder val = (StringBuilder) custhashmap.get(key);
		if(val == null){
	    	val = new StringBuilder();
	    	custhashmap.put(key, val);
	    }
		if(val.length() <= pos/8 )
			val.setLength((pos/8) + 1);
		try{
			log.debug("val : "+ val);
			c = val.charAt(pos / 8);
		}catch(OutOfMemoryError er){
			log.debug("Out Of Memory Error");
			client.addErrorReply(SharedObjs.notIntBit);
			return;
		}
		log.debug("char c : "+c);
		orgVal = (c >> (7 - (pos % 8)) & 1) != 0 ? 1 : 0;  
		log.debug("orgVal : "+ orgVal);
			    
		if(bitval == 1){
			val.setCharAt(pos / 8, (char) (c | 1 << (7- (pos % 8))));
			log.debug("New val : "+ val);
		}
		else if(bitval == 0){
			val.setCharAt(pos / 8, (char) (c & ~(1 << (7- (pos % 8)))));
			log.debug("New val : "+ val);
		}
		
		else{
			client.addErrorReply(SharedObjs.notBit);
			log.debug(SharedObjs.notBit);
			return;
		}
		
		client.setReply(SharedObjs.colon+orgVal+SharedObjs.newLine);
		log.debug(SharedObjs.colon+orgVal);
	}
	
}
