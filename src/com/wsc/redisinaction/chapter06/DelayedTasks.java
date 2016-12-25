package com.wsc.redisinaction.chapter06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class DelayedTasks {
    public static final void main(String[] args)
        throws Exception
    {
        new DelayedTasks().run();
    }

    public void run()
        throws InterruptedException, IOException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        testDelayedTasks(conn);
    }
    public void testDelayedTasks(Jedis conn)
            throws InterruptedException
        {
            System.out.println("\n----- testDelayedTasks -----");
            conn.del("queue:tqueue", "delayed:");
            System.out.println("Let's start some regular and delayed tasks...");
            for (long delay : new long[]{0, 500, 0, 1500}){
                assert executeLater(conn, "tqueue", "testfn", new ArrayList<String>(), delay) != null;
            }
            long r = conn.llen("queue:tqueue");
            System.out.println("How many non-delayed tasks are there (should be 2)? " + r);
            assert r == 2;
            System.out.println();

            System.out.println("Let's start up a thread to bring those delayed tasks back...");
            PollQueueThread thread = new PollQueueThread();
            thread.start();
            System.out.println("Started.");
            System.out.println("Let's wait for those tasks to be prepared...");
            Thread.sleep(2000);
            thread.quit();
            thread.join();
            r = conn.llen("queue:tqueue");
            System.out.println("Waiting is over, how many tasks do we have (should be 4)? " + r);
            assert r == 4;
            conn.del("queue:tqueue", "delayed:");
        }
    
    /**
     * 创建延迟任务
     * 
     * page 137
     * 
     * 清单 6-22
     * 
     * @param conn
     * @param queue
     * @param name
     * @param args
     * @param delay
     * @return
     */
    public String executeLater(
        Jedis conn, String queue, String name, List<String> args, long delay)
    {
    	//Josn列表
        Gson gson = new Gson();
        
        //生成唯一的标识符
        String identifier = UUID.randomUUID().toString();
        
        //准备好需要入队的任务
        String itemArgs = gson.toJson(args);
        String item = gson.toJson(new String[]{identifier, queue, name, itemArgs});
        
        if (delay > 0){
        	//延迟这个任务的执行时间
            conn.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
        	//立即执行这个任务
        	conn.rpush("queue:" + queue, item);
        }
        //返回标识符
        return identifier;
    }

    public String acquireLock(Jedis conn, String lockName) {
        return acquireLock(conn, lockName, 10000);
    }
    /**
     * 使用最基本的方法尝试获得锁，如果失败重新尝试。直到成功
     * 
     * page 119
     * 清单 6-8
     * 
     * @param conn
     * @param lockName
     * @param acquireTimeout
     * @return
     */
    public String acquireLock(Jedis conn, String lockName, long acquireTimeout){
    	//128位随机标识符
        String identifier = UUID.randomUUID().toString();

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end){
            //尝试取得锁
        	//setnx命令的作用，尝试在代表锁的键不存在的情况下，为键设置一个值，以此来获得锁
        	if (conn.setnx("lock:" + lockName, identifier) == 1){
                return identifier;
            }

            try {
                Thread.sleep(1);
            }catch(InterruptedException ie){
                Thread.currentThread().interrupt();
            }
        }

        return null;
    }
    
    /**
     * 释放锁，先使用watch监视代表锁的键，接着检查键目前的值是否和加锁时设置的一样，
     * 并在确认值没有发生变化之后删除该键
     * 
     * page 120
     * 清单 6-9
     * 
     * @param conn
     * @param lockName
     * @param identifier
     * @return
     */
    public boolean releaseLock(Jedis conn, String lockName, String identifier) {
        String lockKey = "lock:" + lockName;

        while (true){
            conn.watch(lockKey);
            
            //检查进程是否仍然持有锁
            if (identifier.equals(conn.get(lockKey))){
                Transaction trans = conn.multi();
                //删除该键，释放锁
                trans.del(lockKey);
                List<Object> results = trans.exec();
                if (results == null){
                    continue;
                }
                return true;
            }

            conn.unwatch();
            break;
        }
        
        //进程已经失去了锁
        return false;
    }
    
    /**
     * 从延迟队列中获取可执行任务
     * 
     * page 138
     * 
     * 
     * @author wsc
     *
     */
    public class PollQueueThread
        extends Thread
    {
        private Jedis conn;
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread(){
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit){
            	//获取任务队列中的第一个任务
                Set<Tuple> items = conn.zrangeWithScores("delayed:", 0, 0);
                Tuple item = items.size() > 0 ? items.iterator().next() : null;
                
                //队列中没有包含任何任务，或者任务的执行时间未到
                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    try{
                        sleep(10);
                    }catch(InterruptedException ie){
                        Thread.interrupted();
                    }
                    continue;
                }
                
                //解码要被执行的任务，弄清楚他应该被推入哪个任务的队列
                String json = item.getElement();
                String[] values = gson.fromJson(json, String[].class);
                String identifier = values[0];
                String queue = values[1];
                
                //获取锁失败，跳过下面的步骤并重试
                String locked = acquireLock(conn, identifier);
                if (locked == null){
                    continue;
                }
                
                //将任务推入适当的任务队列里面
                if (conn.zrem("delayed:", json) == 1){
                    conn.rpush("queue:" + queue, json);
                }
                
                //释放锁
                releaseLock(conn, identifier, locked);
            }
        }
    }

}
