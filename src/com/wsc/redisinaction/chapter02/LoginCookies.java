package com.wsc.redisinaction.chapter02;

import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;

public class LoginCookies {
	public static void main(String[] args) 
			throws InterruptedException
	{
		new LoginCookies().run();
	}
	
	public void run() 
			throws InterruptedException
	{	
		Jedis conn = new Jedis("localhost");
        conn.select(15);
        
        testLoginCookies(conn);
	}
	
	public void testLoginCookies(Jedis conn)
			throws InterruptedException
	{
		System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        updateToken(conn, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'");
        System.out.println();

        System.out.println("What username do we get when we look-up that token?");
        String r = checkToken(conn, token);
        System.out.println(r);
        System.out.println();
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        CleanSessionsThread thread = new CleanSessionsThread(0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()){
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = conn.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
	}
	
	 /**
     * 检查登录cookie
     * 
     * page 25
     * 清单 2-1
     * 
     * @param conn
     * @param token
     * @return
     */
    public String checkToken(Jedis conn, String token) {
    	//尝试获取并返回令牌相应的用户
        return conn.hget("login:", token);
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
     * 
     * 利用线程，定时清理就会话
     * @author wsc
     *
     */
    public class CleanSessionsThread
        extends Thread
    {
        private Jedis conn;
        private int limit;
        private boolean quit;

        public CleanSessionsThread(int limit) {
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
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

                //为那些将要被删除的令牌构建键名
                ArrayList<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }

                //移除最旧的那些令牌
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", tokens);
                conn.zrem("recent:", tokens);
            }
        }
    }
}
