package com.wsc.redisinaction.chapter02;

import java.util.Set;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class CacheRows {

	public static void main(String[] args) 
			throws InterruptedException
	{
		new CacheRows().run();
	}
	
	public void run() 
			throws InterruptedException
	{	
		Jedis conn = new Jedis("localhost");
        conn.select(15);
        
        testCacheRows(conn);
	}
	
    public void testCacheRows(Jedis conn)
            throws InterruptedException
        {
            System.out.println("\n----- testCacheRows -----");
            System.out.println("First, let's schedule caching of itemX every 5 seconds");
            scheduleRowCache(conn, "itemX", 5);
            System.out.println("Our schedule looks like:");
            Set<Tuple> s = conn.zrangeWithScores("schedule:", 0, -1);
            for (Tuple tuple : s){
                System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
            }
            assert s.size() != 0;

            System.out.println("We'll start a caching thread that will cache the data...");

            CacheRowsThread thread = new CacheRowsThread();
            thread.start();

            Thread.sleep(1000);
            System.out.println("Our cached data looks like:");
            String r = conn.get("inv:itemX");
            System.out.println(r);
            assert r != null;
            System.out.println();

            System.out.println("We'll check again in 5 seconds...");
            Thread.sleep(5000);
            System.out.println("Notice that the data has changed...");
            String r2 = conn.get("inv:itemX");
            System.out.println(r2);
            System.out.println();
            assert r2 != null;
            assert !r.equals(r2);

            System.out.println("Let's force un-caching");
            scheduleRowCache(conn, "itemX", -1);
            Thread.sleep(1000);
            r = conn.get("inv:itemX");
            System.out.println("The cache was cleared? " + (r == null));
            assert r == null;

            thread.quit();
            Thread.sleep(2000);
            if (thread.isAlive()){
                throw new RuntimeException("The database caching thread is still alive?!?");
            }
        }

    /**
     * 负责调度缓存和终止缓存的函数
     * 
     * page 32
     * 清单 2-7
     * 
     * 
     * @param conn
     * @param rowId
     * @param delay
     */
    public void scheduleRowCache(Jedis conn, String rowId, int delay) {
    	//先设置数据行的延迟值
        conn.zadd("delay:", delay, rowId);
        
        //立即对需要缓存的数据行进行调度
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
    }
    
    /**
     * 守护进程类
     * 
     * page 32 
     * 清单 2-8
     * 
     * 
     * @author wsc
     *
     */
    public class CacheRowsThread
        extends Thread
    {
        private Jedis conn;
        private boolean quit;

        public CacheRowsThread() {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
        	
        	//Josn格式的数据
            Gson gson = new Gson();
            while (!quit){
                Set<Tuple> range = conn.zrangeWithScores("schedule:", 0, 0);
                
                //尝试获取下一个需要被缓存的数据行以及该行的调度时间戳，
                //命令会返回一个包含零个或一个元组（Tuple）的列表
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if (next == null || next.getScore() > now){
                    try {
                    	//暂时没有行需要被缓存，休眠50毫秒后重试
                        sleep(50);
                    }catch(InterruptedException ie){
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }
                
                String rowId = next.getElement();
                
                //提前获取下一次调度的延迟时间
                double delay = conn.zscore("delay:", rowId);
                
                //不必在缓存这个行，将它从缓存中移除
                if (delay <= 0) {
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }
                
                //读取数据行
                Inventory row = Inventory.get(rowId);
                
                //更新调度时间，并设置缓存值
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }

    public static class Inventory {
        @SuppressWarnings("unused")
		private String id;
        @SuppressWarnings("unused")
		private String data;
        @SuppressWarnings("unused")
		private long time;

        private Inventory (String id) {
            this.id = id;
            this.data = "data to cache...";
            this.time = System.currentTimeMillis() / 1000;
        }

        public static Inventory get(String id) {
            return new Inventory(id);
        }
    }
}
