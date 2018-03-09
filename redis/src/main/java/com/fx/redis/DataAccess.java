package com.fx.redis;

import java.util.List;

public interface DataAccess<T> {

	void set(String key,T value);
	
	void set(String key,T value,int validTime);
	
	T get(String key);
	
	List<T> popAll(String key);
	
	void push(String key,T value);
	
	boolean setNX(String key,T value,int validtime);
	
	void delete(String key);
	
	boolean tryLock(String key,T value,int timeout);
	
	void unLock(String key);
}
