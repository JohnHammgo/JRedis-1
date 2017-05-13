package com.jredis.utilities;

import java.io.Serializable;
import java.util.HashMap;

import com.google.common.collect.TreeMultimap;
import com.google.common.collect.TreeMultiset;

public class CustomList implements Serializable {

	private HashMap<String, Integer> hm;
	private TreeMultiset<Integer> tms;
	private TreeMultimap<Integer, String> tmm;
	
	public HashMap<String,Integer> getHm(){
		return hm;
	}
	public TreeMultiset<Integer> getTms(){
		return tms;
	}
	public TreeMultimap<Integer, String> getTmm(){
		return tmm;
	}
	public void setHm(HashMap<String, Integer> hm){
		this.hm = hm;
	}
	public void setTms(TreeMultiset<Integer> tms){
		this.tms = tms;
	}
	public void setTmm(TreeMultimap<Integer, String> tmm){
		this.tmm = tmm;
	}
	public String toString(){
		return hm + " | "+ tms + " | "+ tmm;
	}
}
