package com.wsc.redisinaction.chapter06;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

public class CountingSemaphore {
	public static void main(String[] args) 
			throws Exception
	{
			 new CountingSemaphore().run();
	}
    public void run()
        	throws InterruptedException, IOException
    {
        	Jedis conn = new Jedis("localhost");
        	conn.select(15);
        	testCountingSemaphore(conn);       
    }
    
    public void testCountingSemaphore(Jedis conn)
            throws InterruptedException
    {
            System.out.println("\n----- testCountingSemaphore -----");
            conn.del("testsem", "testsem:owner", "testsem:counter");
            System.out.println("Getting 3 initial semaphores with a limit of 3...");
            for (int i = 0; i < 3; i++) {
                assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
            }
            System.out.println("Done!");
            System.out.println("Getting one more that should fail...");
            assert acquireFairSemaphore(conn, "testsem", 3, 1000) == null;
            System.out.println("Couldn't get it!");
            System.out.println();

            System.out.println("Lets's wait for some of them to time out");
            Thread.sleep(2000);
            System.out.println("Can we get one?");
            String id = acquireFairSemaphore(conn, "testsem", 3, 1000);
            assert id != null;
            System.out.println("Got one!");
            System.out.println("Let's release it...");
            assert releaseFairSemaphore(conn, "testsem", id);
            System.out.println("Released!");
            System.out.println();
            System.out.println("And let's make sure we can get 3 more!");
            for (int i = 0; i < 3; i++) {
                assert acquireFairSemaphore(conn, "testsem", 3, 1000) != null;
            }
            System.out.println("We got them!");
            conn.del("testsem", "testsem:owner", "testsem:counter");
    }    
    
    /**
     * 公平信号量的获取，比较复杂。
     * 首先有序集合移除超时信号量，然后超时集合与信号量拥有者集合并集计算
     * 之后对计数器自增操作，将计数器生成的值添加到信号量拥有者有序集合
     * 
     * page 129
     * 清单 6-114
     * 
     * @param conn
     * @param semname
     * @param limit
     * @param timeout
     * @return
     */
    public String acquireFairSemaphore(
        Jedis conn, String semname, int limit, long timeout)
    {
    	//128位随机标识符
        String identifier = UUID.randomUUID().toString();
        String czset = semname + ":owner";
        String ctr = semname + ":counter";

        long now = System.currentTimeMillis();
        Transaction trans = conn.multi();
        //删除超时的信号量
        trans.zremrangeByScore(
            semname.getBytes(),
            "-inf".getBytes(),
            String.valueOf(now - timeout).getBytes());
        
        ZParams params = new ZParams();
        params.weights(1, 0);
        //超时有序集合与信号量拥有者有序集合执行并集计算，
        //并将计算结果保存到信号量拥有者有序集合里面
        //覆盖有序集合原有的数据
        trans.zinterstore(czset, params, czset, semname);
        //对计数器执行自增操作，并获取计数器在执行自增操作之后的值
        trans.incr(ctr);
        List<Object> results = trans.exec();
        int counter = ((Long)results.get(results.size() - 1)).intValue();

        trans = conn.multi();
        //尝试获取信号量
        trans.zadd(semname, now, identifier);
        trans.zadd(czset, counter, identifier);
        
        //通过检查排名来判断客户端是否取得了信号量
        trans.zrank(czset, identifier);
        results = trans.exec();
        int result = ((Long)results.get(results.size() - 1)).intValue();
        //检查是否成功的取得了信号量
        if (result < limit){
            return identifier;
        }

        trans = conn.multi();
        //获取信号量失败，删除之前添加的标识符
        trans.zrem(semname, identifier);
        trans.zrem(czset, identifier);
        trans.exec();
        return null;
    }

    /**
     * 信号量的释放，程序只需要从有序集合里面移除指定的标识符
     * 如果信号量已经被正确的释放掉，那么返回true；
     *返回false则表示该信号量已经因为过期而被删除了
     * 
     * page 128
     * 清单6-13
     * 
     * @param conn
     * @param semname
     * @param identifier
     * @return
     */
    public boolean releaseFairSemaphore(
        Jedis conn, String semname, String identifier)
    {
    	//信号量的释放，程序只需要从有序集合里面移除指定的标识符
        Transaction trans = conn.multi();
        trans.zrem(semname, identifier);
        trans.zrem(semname + ":owner", identifier);
        List<Object> results = trans.exec();
        
        //如果信号量已经被正确的释放掉，那么返回true；
        //返回false则表示该信号量已经因为过期而被删除了
        return (Long)results.get(results.size() - 1) == 1;
    }
}
