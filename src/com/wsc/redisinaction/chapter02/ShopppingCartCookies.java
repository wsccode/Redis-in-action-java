package com.wsc.redisinaction.chapter02;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;

public class ShopppingCartCookies {

	
	public static void main(String[] args) 
			throws InterruptedException
	{
		new ShopppingCartCookies().run();
	}
	
	public void run() 
			throws InterruptedException
	{	
		Jedis conn = new Jedis("localhost");
        conn.select(15);
        
        testShopppingCartCookies(conn);
	}
	
	public void testShopppingCartCookies(Jedis conn)
	    throws InterruptedException
	{
	    System.out.println("\n----- testShopppingCartCookies -----");
	    String token = UUID.randomUUID().toString();

	    System.out.println("We'll refresh our session...");
	    updateToken(conn, token, "username", "itemX");
	    System.out.println("And add an item to the shopping cart");
	    addToCart(conn, token, "itemY", 3);
	    Map<String,String> r = conn.hgetAll("cart:" + token);
	    System.out.println("Our shopping cart currently has:");
	    for (Map.Entry<String,String> entry : r.entrySet()){
	         System.out.println("  " + entry.getKey() + ": " + entry.getValue());
	    }
	    System.out.println();

	    assert r.size() >= 1;

	    System.out.println("Let's clean out our sessions and carts");
	    CleanFullSessionsThread thread = new CleanFullSessionsThread(0);
	    thread.start();
	    Thread.sleep(1000);
	    thread.quit();
	    Thread.sleep(2000);
	    if (thread.isAlive()){
	        throw new RuntimeException("The clean sessions thread is still alive?!?");
	    }

	    r = conn.hgetAll("cart:" + token);
	    System.out.println("Our shopping cart now contains:");
	    for (Map.Entry<String,String> entry : r.entrySet()){
	        System.out.println("  " + entry.getKey() + ": " + entry.getValue());
	    }
	    assert r.size() == 0;
	}

	/**
     * 程序更新令牌
     * 
     * page 25
     * 清单2-2
     * 
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item) {
    	//获取当前的时间戳
        long timestamp = System.currentTimeMillis() / 1000;
        
        //维持令牌与以登录用户之间的映射
        conn.hset("login:", token, user);
        
        //记录令牌最后一次出现的时间
        conn.zadd("recent:", timestamp, token);
        
        if (item != null) {
        	//记录用户浏览过的商品
        	conn.zadd("viewed:" + token, timestamp, item);
            
        	//移除旧的记录，只保留用户最近浏览过的25个商品
        	conn.zremrangeByRank("viewed:" + token, 0, -26);
            conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 更新购物车
     * 
     * page 28
     * 清单 2-4
     * 
     * @param conn
     * @param session
     * @param item
     * @param count
     */
    public void addToCart(Jedis conn, String session, String item, int count) {
        if (count <= 0) {
        	//从购物车中移除指定的商品
            conn.hdel("cart:" + session, item);
        } else {
        	//将指定的商品添加到购物车中
            conn.hset("cart:" + session, item, String.valueOf(count));
        }
    }
    
    /**
     * 线程定时更新，当清理旧的会话时，将旧会话对应的购物车也一并删除
     * 
     * @author wsc
     *
     */
    public class CleanFullSessionsThread
        extends Thread
    {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanFullSessionsThread(int limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.limit = limit;
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit) {
            	//找出目前已有令牌的数量
                long size = conn.zcard("recent:");
                
                //令牌的数量未超过限制，休眠并在之后重新检查
                if (size <= limit){
                    try {
                        sleep(1000);
                    }catch(InterruptedException ie){
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                //获取需要移除的令牌ID
                long endIndex = Math.min(size - limit, 100);
                Set<String> sessionSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] sessions = sessionSet.toArray(new String[sessionSet.size()]);

                //为那些将要被删除的令牌构建键名
                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String sess : sessions) {
                    sessionKeys.add("viewed:" + sess);
                    
                    //用于删除旧会话对应用的购物车
                    sessionKeys.add("cart:" + sess);
                }

                //移除最旧的那些令牌
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", sessions);
                conn.zrem("recent:", sessions);
            }
        }
    }

}
