package com.fx.redis;

import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import redis.clients.jedis.Jedis;

public class RedisAccessBuilder {

	private static final Jedis jedis = new Jedis(Cantans_Redis.REDIS_HOST,Cantans_Redis.REDIS_PORT);
	
	public static void main(String[] args) {

//		testOption();
//		jedis.set("test", "hello redis");
//		System.out.println(jedis.get("test"));
//		System.out.println(jedis.del("test"));
//		System.out.println(jedis.get("test"));
//		testString();
//		testMap();
//		testList();
//		testSet();
//		testZSet();
		testSort();
	}
	
	/**
	 * 
	 * */
	@SuppressWarnings("unused")
	private static void testOption() {
		System.out.println(jedis.ping());
		String s = jedis.flushDB();
		System.out.println("flush this rdis :"+s);
		
		System.out.println(jedis.echo("foo"));
		jedis.set("foo", "存储变量foo");
		jedis.set("ttt", "存储变量ttt");
		jedis.expire("ttt", 10);
		
		Boolean is = jedis.exists("foo");
		System.out.println("foo is exists:"+is);
		
		Set<String> keys = jedis.keys("*");
		System.out.println("db key is:"+keys);
		
		Set<String> keys1 = jedis.keys("f*");
		System.out.println("数据库中以f为前缀的keys："+keys1);
		
		String foo = jedis.type("foo");
		System.out.println("foo的类型："+foo);
		
		jedis.del("foo");
		System.out.println("key is :"+jedis.keys("*"));
	}
	
	public static void testString() {
		jedis.flushDB();
		jedis.set("name", "miao");
		System.out.println("取出name："+jedis.get("name"));
		
		String name = jedis.getSet("name", "被修改的值");
		System.out.println("取出:"+name+" 修改:"+jedis.get("name"));
		String name1 = jedis.getrange("name", 0, 2);
		try {
			System.out.println("获取value值并截取（中文乱码）："+name1.getBytes("GBK").length);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		jedis.set("name", "覆盖了！！！");
		System.out.println("取出重设后的name："+jedis.get("name"));
		//拼接
		jedis.append("name", "is stronger");
		System.out.println(""+jedis.get("name"));
		
		jedis.del("name");
		System.out.println("删除后的name："+jedis.get("name"));
		
		jedis.mset("name","miao","age","50","birth","1991-02-22");
		jedis.incr("age");
		System.out.println(jedis.get("name")+"-"+jedis.get("age")+"-"+jedis.get("birth"));
		
		jedis.del("name","age","birth");
	}
	
	private static void testMap() {
		jedis.flushDB();
		
		Map<String, String > map = new HashMap<String, String>();
		map.put("name", "miao");
		map.put("age", "25");
		map.put("birth", "1991-05-28");
		
		jedis.hmset("user", map);
		
		List<String> user = jedis.hmget("user","name","age","birth");
		System.out.println(user);
		Map<String, String> user1 = jedis.hgetAll("user");
		System.out.println("user1:"+user1);
		
		jedis.hdel("user", "birth");
		System.out.println(jedis.hmget("user", "name","age","birth"));
		
		Long count = jedis.hlen("user");
		System.out.println("属性的个数："+count);
		
		Boolean is = jedis.exists("user");
		System.out.println("是否存在：" +is);
		
		Set<String> ukeys = jedis.hkeys("user");
		System.out.println("ukeys"+ukeys);
		
		List<String> uvals = jedis.hvals("user");
		System.out.println("uvals:"+uvals);
		
		jedis.del("user");
	}
	
	private static void testList() {
		jedis.flushDB();
		
		List<String> user = jedis.lrange("users", 0, -1);
		System.out.println("list:"+user);
		
		//向左插入
        jedis.lpush("users","miao");
        jedis.lpush("users","rui");
        jedis.lpush("users","feng");
        //向右插入
        jedis.rpush("users","ming");
        jedis.rpush("users","hong");
        jedis.rpush("users","xing");
        
        System.out.println("插入的结果："+jedis.lrange("users", 0, -1));
        
        Long len = jedis.llen("users");
        System.out.println("list长度:"+len);
        
        System.out.println("取出索引0到1的数据："+jedis.lrange("users", 0, 1));
        
        jedis.lset("users", 0, "我变成第一个");
        System.out.println("修改单个值后："+jedis.lrange("users", 0, -1));
        
        String users1 = jedis.lindex("users", 0);
        System.out.println("下标为0："+users1);
        
        Long lrem = jedis.lrem("users", 0, "我变成第一个");
        System.out.println("删除下标0的结果："+lrem+" | "+ jedis.lrange("users", 0, -1));
        
        String users2 = jedis.lpop("users");
        System.out.println("左出栈："+users2+"|"+jedis.lrange("users", 0, -1));
        
        String rpop = jedis.rpop("users");
        System.out.println("右出栈："+rpop+"|"+jedis.lrange("users", 0, -1));
        
        jedis.del("users");
        System.out.println(jedis.lrange("user", 0, -1));
        
	}

	private static void testSet() {
		jedis.sadd("user", "maio");
		jedis.sadd("user", "rui");
		jedis.sadd("user", "feng");
		jedis.sadd("user", "ming");
		jedis.sadd("user", "hong");
		jedis.sadd("user", "xing");
		//查看
		Set<String> user = jedis.smembers("user");
		System.out.println("set: "+user);
		//移除
		jedis.srem("user", "miao");
		System.out.println("移除后："+jedis.smembers("user"));
		//判断是否存在
		Boolean is = jedis.sismember("user", "rui");
		System.out.println("rui时候存在："+is);
		//随机取出一个
		String one = jedis.srandmember("user");
		System.out.println(one);
		
		System.out.println(jedis.srandmember("user", 2));
		//返回个数
		Long count = jedis.scard("user");
		System.out.println("user 个数："+count);
		
		jedis.del("user");
		
	}
	
	private static void testZSet() {
		jedis.flushDB();
		jedis.zadd("zkey", 12.1, "十二");
		jedis.zadd("zkey", 11.1, "11");
		jedis.zadd("zkey", 21.1, "20");
		jedis.zadd("zkey", 41.1, "40");
		jedis.zadd("zkey", 41.1, "50");
		jedis.zadd("zkey", 4, "4");
		
		Set<String> zkey = jedis.zrange("zkey", 0, -1);
		System.out.println("可以看到是以socore排序的："+zkey);
	}
	
	private static void testSort() {
		
		jedis.del("list");
		
		jedis.rpush("list", "1");
		jedis.rpush("list", "4");
		jedis.rpush("list", "2");
		jedis.rpush("list", "20");
		jedis.rpush("list", "5");
		jedis.rpush("list", "6");
		jedis.rpush("list", "67");
		jedis.rpush("list", "56");
		
		System.out.println(jedis.lrange("list", 0, -1));
		System.out.println("排序："+jedis.sort("list"));
		System.out.println("排序不影响原存储："+jedis.lrange("list", 0, -1));
		
	}
}
