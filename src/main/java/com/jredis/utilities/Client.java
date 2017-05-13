package com.jredis.utilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.jredis.server.RedisDb;
import com.jredis.server.RedisServer;

public class Client {

	private SocketChannel socketChannel;
	private SelectionKey selectKey;
	private String reply;
		
	/**********
	 * 
	 * SETTERS AND GETTERS
	 * 
	 */
	public static Client newInstance(){
		return new Client();
	}
	public void setChannel(ServerSocketChannel ServSock) throws IOException{
		socketChannel = ServSock.accept();
		socketChannel.configureBlocking(false);
	}
	public void setChannel(SocketChannel socketChannel){
		this.socketChannel = socketChannel;
	}
	public void setSelectKeyForRead(Selector selector) throws ClosedChannelException{
		selectKey = socketChannel.register(selector, SelectionKey.OP_READ);
		//attach a buffer with client
		ByteBuffer buf = ByteBuffer.allocate(2096);
		selectKey.attach(buf);
	}
	public void setSelectKeyForWrite(Selector selector) throws ClosedChannelException{
		selectKey = socketChannel.register(selector, SelectionKey.OP_WRITE);
		//attach a buffer with client
		ByteBuffer buf = ByteBuffer.allocate(256);
		selectKey.attach(buf);
	}
	public SocketChannel getChannel(){
		return socketChannel;
	}
	public SelectionKey getSelectionKey(){
		return selectKey;
	}
	public void setReply(String reply) throws ClosedChannelException{
		this.reply = reply;
		setSelectKeyForWrite(RedisServer.selector);
	}
	public String getReply(){
		return reply;
	}
	
	//Other methods
	public void addErrorReply(String reply) throws ClosedChannelException{
		setReply(SharedObjs.err+ " "+ reply);
	}
}
