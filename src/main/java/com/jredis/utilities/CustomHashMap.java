
package com.jredis.utilities;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CustomHashMap<K, V> implements Map<K, V>, Serializable {
	 
	private final HashMap<K, V> store = new HashMap<K, V>();
	private final HashMap<K, Long> timestamps = new HashMap<K, Long>();
	private final HashMap<K, Long> timetolive = new HashMap<K, Long>();
	
	public V get(Object key){
		V value = this.store.get(key);
		
		if(value != null && expired(key, value)){
			store.remove(key);
			timestamps.remove(key);
			timetolive.remove(key);
			return null;
		}else{
			return value;
		}
	}
	
	private boolean expired(Object key, V value){
		return (System.currentTimeMillis() - timestamps.get(key)) > timetolive.get(key);
	}
	
	public V put(K key, V value){
		timestamps.put(key, System.currentTimeMillis());
		timetolive.put(key, (long) Double.POSITIVE_INFINITY);
		return store.put(key, value);
	}
	
	public V putExpire(K key, V value, Long time){
		timestamps.put(key, System.currentTimeMillis());
		timetolive.put(key, time);
		return store.put(key, value);
	}
	
	public int size(){
		return store.size();
	}
	
	public boolean isEmpty(){
		return store.isEmpty();
	}
	
	public boolean containsKey(Object key){
		return store.containsKey(key);
	}
	
	public boolean containsValue(Object value){
		return store.containsValue(value);
	}
	
	public V remove(Object key){
		timestamps.remove(key);
		timetolive.remove(key);
		return store.remove(key);
	}
	
	public void putAll(Map<? extends K, ? extends V> m){
		for(Map.Entry<? extends K,? extends V> e : m.entrySet()){
			this.put(e.getKey(), e.getValue());
		}
	}
	
	public void clear(){
		timestamps.clear();
		timetolive.clear();
		store.clear();
	}
	
	public Set<K> keySet(){
		clearExpired();
		return store.keySet();
	}
	
	public Collection<V> values(){
		clearExpired();
		return store.values();
	}
	
	public Set<Map.Entry<K, V>> entrySet(){
		clearExpired();
		return store.entrySet();
	}
	
	public String toString(){
		return store.toString();
	}
	
	private void clearExpired(){
		for(K k: store.keySet()){
			this.get(k);
		}
	}
	
}

