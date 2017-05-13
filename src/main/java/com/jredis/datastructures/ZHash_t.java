package com.jredis.datastructures;

import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import com.jredis.utilities.CustomHashMap;
import com.google.common.collect.BoundType;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;
import com.jredis.server.RedisCommand;
import com.jredis.server.RedisServer;
import com.jredis.utilities.Client;
import com.jredis.utilities.CustomList;
import com.jredis.utilities.SharedObjs;

public class ZHash_t {
	
	final static Logger log = Logger.getLogger(String_T.class);
	private CustomHashMap<String, Object> custhashmap;
	private HashMap<String, Object> hashmap;
	
	public static ZHash_t newInstance(Client client){
		return new ZHash_t(client);
	}
	public ZHash_t(Client client){
		custhashmap = RedisServer.getDb().getCustHashMap();
		hashmap = RedisServer.getDb().getHashMap();
		log.debug("Custom Hash Map :"+ custhashmap );
		log.debug("Hash Map :"+ hashmap );
	}
	public void zadd(Client client, RedisCommand cmd) throws ClosedChannelException{
		String[] args = cmd.getArgs();
		String key = args[0];
		log.debug("args : "+args+" key : "+key);
		if(custhashmap.get(key) != null){
			client.addErrorReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		int score = 0;
		try{
			score = Integer.parseInt(args[1]);
			log.debug("score : "+ score);
		}catch(NumberFormatException ex){
			client.addErrorReply(SharedObjs.notFloat);
			log.debug(SharedObjs.notFloat);
			return;
		}
		
		String val = args[2];
		log.debug("val :"+val);
		CustomList List = (CustomList) hashmap.get(key);
		if(List == null){
			log.debug("List is null");
			List = new CustomList();
			List.setHm(new HashMap<String, Integer>());
			log.debug("inner HashMap set");
			
			TreeMultiset<Integer> tms = TreeMultiset.create();
			List.setTms(tms);
			log.debug("inner TreeMultiSet set");
			
			TreeMultimap<Integer, String> tmm = TreeMultimap.create();
			List.setTmm(tmm);
			log.debug("TreeMultiMap set");
		}
		
		HashMap<String, Integer> innerHm = List.getHm();
		TreeMultiset<Integer> innerSortedSet = List.getTms();
		TreeMultimap<Integer, String> innerMultiMap = List.getTmm();
		
		Integer initInnerScore = innerHm.remove(val);
		log.debug("Init inner score : "+initInnerScore);
		if(initInnerScore != null){
			innerSortedSet.remove(initInnerScore);
			innerMultiMap.remove(initInnerScore, val);
			log.debug("init inner score : "+initInnerScore+" deleted");
		}
		innerSortedSet.add(score);
		innerMultiMap.put(score, val);
		innerHm.put(val, score);
		log.debug("Score updated : "+score);
		
		List.setHm(innerHm);
		log.debug("inner hm set : "+innerHm);
		List.setTms(innerSortedSet);
		log.debug("inner sorted set updated : "+innerSortedSet);
		List.setTmm(innerMultiMap);
		log.debug("inner Multimap updated : "+innerMultiMap);
		
		hashmap.put(key, List);
		log.debug("Main Hashmap updated");
		if(initInnerScore != null && initInnerScore == score){
			client.setSelectKeyForWrite(RedisServer.selector);
			client.setReply(SharedObjs.zero);
			log.debug(SharedObjs.zero);
		}else{
			client.setSelectKeyForWrite(RedisServer.selector);
			client.setReply(SharedObjs.one);
			log.debug(SharedObjs.one);
		}
	}
	public void zrange(Client client, RedisCommand cmd) throws ClosedChannelException{
		
		String[] args = cmd.getArgs();
		String key = args[0];
		
		log.debug("args[] : "+args+" key : "+key);
		if(custhashmap.get(key) != null){
			client.addErrorReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		
		int startPos = 0;
		int endPos = 0;
		try{
			startPos = Integer.parseInt(args[1]);
			endPos = Integer.parseInt(args[2]);
			log.debug("startPos : "+startPos +" endPos : "+endPos);
		}catch(NumberFormatException ex){
			client.addErrorReply(SharedObjs.notInt);
			log.debug(SharedObjs.notInt);
			return;
		}
		
		CustomList List = (CustomList) hashmap.get(key);
		if(List == null){
			client.setReply(SharedObjs.emptyMultiBulk);
			log.debug(SharedObjs.emptyMultiBulk);
			return;
		}
		SortedMap<Integer, Collection<String>> skipListMap = List.getTmm().asMap();
		log.debug("SortedMap fetched from List");
		if(startPos < 0){
			startPos = -startPos;
			if(startPos <= skipListMap.size())
				startPos = skipListMap.size() - startPos;
			else
				startPos = 0;
		}
		log.debug("New startpos : "+startPos);
		if(endPos < 0){
			endPos = -endPos;
			if(endPos <= skipListMap.size())
				endPos = skipListMap.size() - endPos;
			else
				endPos = 0;
		}
		log.debug("New endpos : "+endPos);
		
		int beg;
		int firstEntry = skipListMap.firstKey();
		log.debug("firstEntry : "+firstEntry);
		if(startPos < firstEntry)
			beg = firstEntry;
		else
			beg = startPos;
		
		log.debug("Beg : "+ beg);
		int end;
		int lastEntry = skipListMap.lastKey();
		log.debug("lastEntry : "+lastEntry);
		if(endPos > lastEntry)
			end = lastEntry;
		else
			end = endPos;
		log.debug("End : "+end);
		
		if(beg >= end){
			client.setReply(SharedObjs.emptyMultiBulk);
			log.debug(SharedObjs.emptyMultiBulk);
			return;
		}
		SortedMap<Integer, Collection<String>> subMap = skipListMap.subMap(beg,end + 1);
		log.debug("Submap created : "+ subMap);
		String reply = "";
		int count = 0;
		
		for(Map.Entry<Integer, Collection<String>> entry :subMap.entrySet()){
			Collection<String> valList = entry.getValue();
			Iterator<String> i = valList.iterator();
			String val = "";
			while(i.hasNext()){
				count++;
				val = i.next();
				reply += "\n"+"$"+val.length()+"\n"+val;
			}
		}
		reply = "*"+count+reply;
		log.debug("Reply : "+ reply);
		client.setReply(reply+"\n");
	}
	public void zcard(Client client, RedisCommand cmd) throws ClosedChannelException{
		String[] args = cmd.getArgs();
		String key = args[0];
		
		log.debug("args[] : "+args+" key: "+key);
		if(custhashmap.get(key) != null){
			client.addErrorReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		
		CustomList List = (CustomList) hashmap.get(key);
		log.debug("List : "+List);
		if(List == null){
			client.setReply(SharedObjs.zero);
			return;
		}
		TreeMultiset<Integer> innerSortedSet
							= (TreeMultiset<Integer>) List.getTms();
		log.debug("Reply : "+ innerSortedSet.size());
		client.setReply(SharedObjs.colon+innerSortedSet.size()+"\n"); 
	}
	public void zcount(Client client, RedisCommand cmd) throws ClosedChannelException{
		String[] args = cmd.getArgs();
		String key = args[0];
		log.debug("args[] : "+args+" key: "+ key);
		if(custhashmap.get(key) != null){
			client.setSelectKeyForWrite(RedisServer.selector);
			client.addErrorReply(SharedObjs.wrongTypeErr);
			log.debug("wrong type error");
			return;
		}
		
		int startPos = 0;
		int endPos = 0;
	    try{
			startPos = Integer.parseInt(args[1]);
		    endPos = Integer.parseInt(args[2]);
		    log.debug("startPos : "+startPos+" endPos : "+endPos);
	    }catch(NumberFormatException ex){
	    	client.setSelectKeyForWrite(RedisServer.selector);
			client.setReply(SharedObjs.znotFloat);
			log.debug(SharedObjs.znotFloat);
			return;
	    }
	    CustomList List = (CustomList) hashmap.get(key);
		if(List == null){
			client.setSelectKeyForWrite(RedisServer.selector);
			client.setReply(SharedObjs.zero);
			log.debug(SharedObjs.zero);
			return;
		}

		TreeMultiset<Integer> innerSortedSet=  List.getTms();
		log.debug("Inner Sorted Set : "+ innerSortedSet);
		int beg;
		int firstEntry = innerSortedSet.firstEntry().getElement();
		
		if(startPos < innerSortedSet.firstEntry().getElement())
			beg = firstEntry;
		else
			beg = startPos;
		log.debug("beg : "+ beg);
		int end;
		int lastEntry = innerSortedSet.lastEntry().getElement();
		
		if(endPos > lastEntry)
			end = lastEntry;
		else
			end = endPos;
		log.debug("end : "+end);		
		int count = innerSortedSet.subMultiset(beg, BoundType.CLOSED
						, end, BoundType.CLOSED).size();

		client.setReply(SharedObjs.colon+count);
		log.debug("count : "+count);
	}
}
