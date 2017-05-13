package com.jredis.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.jredis.utilities.Client;
import com.jredis.utilities.SharedObjs;

public class Networking {

	//handling connections
	//reading command - readQueryFromClient
	//executed only one time - static
	private static RedisServer server;
	
	final static Logger log = Logger.getLogger(Networking.class);
	
	public static void main(String[] args) throws IOException, ClassNotFoundException{
		
		//Initializing server object
		server = RedisServer.newInstance();
		server.setPort(4324);
		server.setSelector();
		server.setServSock();
		log.debug("Server parameters set");
		
		//Defining selector
		ServerSocketChannel servSock = server.getServSock();
		Selector selector = server.getSelector();
		log.debug("Selector Created");
		
		//Binding server to port
		InetSocketAddress addr = new InetSocketAddress(server.getPort());
		servSock.bind(addr);
		log.debug("Server bound to port : "+ server.getPort());
		
		//load Db from disk
		int status =0 ;
		try{
			status = readDbPath(args[0]);
		}catch(ArrayIndexOutOfBoundsException ex){
			System.out.println(SharedObjs.noDb);
			server.getServSock().close();
			System.exit(0);
		}
		if(status == -1){
			server.getServSock().close();
			System.exit(0);
		}
		log.debug("Db path read successfully");
				
		HashMap<SocketChannel, Client> clientMap = new HashMap<SocketChannel, Client>();
		log.debug("Hashmap storing clients against channels created");
		
		SelectionKey key = servSock.register(selector, SelectionKey.OP_ACCEPT);
		log.debug("Selection key set for accept");
		while(true){
			log.debug("Entered selector loop");
			int readyChannels = selector.select();
			if(readyChannels == 0){
				log.debug("No ready channels");
				continue;
			}
			
			Set<SelectionKey> keyList = selector.selectedKeys();
			Iterator<SelectionKey> i = keyList.iterator();
			
			//Server waiting for clients and reading query
			while(i.hasNext()){
				SelectionKey myKey = i.next();
				log.debug("A new key selected");
				if(myKey.isAcceptable()){
					log.debug("Key is acceptable");					
					//Register clients
					ServerSocketChannel channel = (ServerSocketChannel) myKey.channel();
					
					Client client = Client.newInstance();
				    client.setChannel(channel);
					client.setSelectKeyForRead(selector);
						
					
					log.debug("Client initialized successfully");
					log.debug("Connection accepted from "+ client.getChannel().getRemoteAddress());
					
					clientMap.put(client.getChannel(), client);
										
									    
				}else if(myKey.isReadable()){
					
					log.debug("Key is readable");
					
					SocketChannel channel = (SocketChannel) myKey.channel();
					Client client = clientMap.get(channel);
					log.debug("Client fetched from client map : "+ client.getChannel().getRemoteAddress());
					//read query from clients
					//ByteBuffer buf = (ByteBuffer) myKey.attachment();
					readQueryFromClient(client);
					
					log.debug("Command read and sent for processing");

//					channel.write(ByteBuffer.wrap(("Resp - "+req).getBytes()));
//					buf.clear();
														
				}else if(myKey.isWritable()){
					
					log.debug("Key is writable");
					SocketChannel channel = (SocketChannel) myKey.channel();
					Client client = clientMap.get(channel);
					log.debug("Client fetched from client map : "+ client.getChannel().getRemoteAddress());
//					client.setChannel((SocketChannel) myKey.attachment());
//					SocketChannel channel = client.getChannel();
					
//					channel.write(ByteBuffer.wrap(("Resp - "+reply).getBytes()));
//					client.setSelectKeyForRead(RedisServer.selector);
					ByteBuffer bufResp = (ByteBuffer) client.getSelectionKey().attachment();
					bufResp = ByteBuffer.wrap((client.getReply()).getBytes());
					channel.write(bufResp);
					client.setSelectKeyForRead(selector);
				}
				i.remove();
			}
		}
	
	}
	private static void readQueryFromClient(Client client) throws IOException{
		log.debug("Reached read query from client");
		SocketChannel channel = client.getChannel();
		ByteBuffer buffer = (ByteBuffer) client.getSelectionKey().attachment();
		channel.read(buffer);
		buffer.flip();
		String request = "";
		while(buffer.hasRemaining()){
			request +=(char)buffer.get();	
		}	
		log.debug("Request from client : "+ request);
		//process query and pass corresponding map
		server.processCommand(request, client);	
	}
	private static int readDbPath(String dbPath) throws IOException, ClassNotFoundException{
		
		if(!dbPath.substring(dbPath.lastIndexOf('.')).equals(".jrdb")){
			System.out.println(SharedObjs.invalidDb);
			return -1;
		}
		System.out.println(SharedObjs.loadingDb);
		RedisDb db = RedisDb.newInstance(dbPath);
		db.getDbFromDisk();
		server.setDb(db);;
		log.debug("Db added to server");
		return 1;
	}
}	
