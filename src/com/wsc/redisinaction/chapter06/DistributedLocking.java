package com.wsc.redisinaction.chapter06;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class DistributedLocking {
	public static void main(String[] args) 
		throws Exception
	{
		 new DistributedLocking().run();
	}
    public void run()
    	throws InterruptedException, IOException
    {
    	Jedis conn = new Jedis("localhost");
    	conn.select(15);
    	testDistributedLocking(conn);       
    }
    
    /**
     * 测试添加了超时时间设置的 
     * 
     * 
     * @param conn
     * @throws InterruptedException
     */
    public void testDistributedLocking(Jedis conn)
            throws InterruptedException
    {
            System.out.println("\n----- testDistributedLocking -----");
            conn.del("lock:testlock");
            System.out.println("Getting an initial lock...");
            assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
            System.out.println("Got it!");
            System.out.println("Trying to get it again without releasing the first one...");
            assert acquireLockWithTimeout(conn, "testlock", 10, 1000) == null;
            System.out.println("Failed to get it!");
            System.out.println();

            System.out.println("Waiting for the lock to timeout...");
            Thread.sleep(2000);
            System.out.println("Getting the lock again...");
            String lockId = acquireLockWithTimeout(conn, "testlock", 1000, 1000);
            assert lockId != null;
            System.out.println("Got it!");
            System.out.println("Releasing the lock...");
            assert releaseLock(conn, "testlock", lockId);
            System.out.println("Released it...");
            System.out.println();

            System.out.println("Acquiring it again...");
            assert acquireLockWithTimeout(conn, "testlock", 1000, 1000) != null;
            System.out.println("Got it!");
            conn.del("lock:testlock");
    }
    /**
     * 添加了超时时间的设置的锁
     * 
     * page 125
     * 清单 6-11
     * 
     * @param conn
     * @param lockName
     * @param acquireTimeout
     * @param lockTimeout
     * @return
     */
    public String acquireLockWithTimeout(
        Jedis conn, String lockName, long acquireTimeout, long lockTimeout)
    {
    	//128位随机标识符
        String identifier = UUID.randomUUID().toString();
        String lockKey = "lock:" + lockName;
        
        //利用强制转换，确保传给EXPIRE的都是整数
        int lockExpire = (int)(lockTimeout / 1000);

        long end = System.currentTimeMillis() + acquireTimeout;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockKey, identifier) == 1){
            	//获取锁，并设置过期时间
                conn.expire(lockKey, lockExpire);
                return identifier;
            }
            //检查过期时间，并在需要时对其进行更新
            if (conn.ttl(lockKey) == -1) {
                conn.expire(lockKey, lockExpire);
            }

            try {
                Thread.sleep(1);
            }catch(InterruptedException ie){
                Thread.currentThread().interrupt();
            }
        }

        // null indicates that the lock was not acquired
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
 
    
}
