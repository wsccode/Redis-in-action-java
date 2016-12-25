package com.wsc.redisinaction.chapter04;

import java.lang.reflect.Method;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class BenchmarkUpdateToken {
	public static final void main(String[] args) {
        new BenchmarkUpdateToken().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testBenchmarkUpdateToken(conn);
    }

    public void testBenchmarkUpdateToken(Jedis conn) {
        System.out.println("\n----- testBenchmarkUpdate -----");
        benchmarkUpdateToken(conn, 5);
    }
    
    /**
     *性能测试函数，在给定的时间内重复执行 updateToken方法和updateTokenPipeline 
     *然后计算被测试的方法美妙执行了多少次
     *
     *page 84
     *清单 4-9
     * 
     * 
     * @param conn
     * @param duration
     */
    public void benchmarkUpdateToken(Jedis conn, int duration) {
        try{
            @SuppressWarnings("rawtypes")
            Class[] args = new Class[]{
                Jedis.class, String.class, String.class, String.class};
            Method[] methods = new Method[]{
                this.getClass().getDeclaredMethod("updateToken", args),
                this.getClass().getDeclaredMethod("updateTokenPipeline", args),
            };
            
            //测试会分别执行updateToken方法和updateTokenPipeline方法
            for (Method method : methods){
            	
            	//设置计数器以及测试结束的条件
                int count = 0;
                long start = System.currentTimeMillis();
                long end = start + (duration * 1000);
                while (System.currentTimeMillis() < end){
                    count++;
                    //调用两个方法中的一个
                    method.invoke(this, conn, "token", "user", "item");
                }
                
                //计算方法的执行时长
                long delta = System.currentTimeMillis() - start;
                
                //打印测试结果
                System.out.println(
                        method.getName() + ' ' +
                        count + ' ' +
                        (delta / 1000) + ' ' +
                        (count / (delta / 1000)));
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }
    /**
     * 负责记录用户最近浏览过的商品以及最近访问过的页面
     * 
     * page 83
     * 清单 4-7
     * 
     * 
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateToken(Jedis conn, String token, String user, String item) {
    	//获取时间戳
        long timestamp = System.currentTimeMillis() / 1000;
        
        //创建令牌与已登录用户之间的映射
        conn.hset("login:", token, user);
        
        //记录令牌最后出现的时间
        conn.zadd("recent:", timestamp, token);
       
        if (item != null) {
        	//把用户浏览过的商品记录起来
        	conn.zadd("viewed:" + token, timestamp, item);
            
        	//移除旧商品，只记录最新浏览的25件商品
        	conn.zremrangeByRank("viewed:" + token, 0, -26);
            
        	//更新给定商品的被浏览次数
        	conn.zincrby("viewed:", -1, item);
        }
    }

    /**
     * 创建一个非事务型流水线，然后使用流水线来发送所有的请求
     * 
     * page 84
     * 
     * 清单 4-8
     * 
     * @param conn
     * @param token
     * @param user
     * @param item
     */
    public void updateTokenPipeline(Jedis conn, String token, String user, String item) {
        long timestamp = System.currentTimeMillis() / 1000;
        
        //设置流水线
        Pipeline pipe = conn.pipelined();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null){
            pipe.zadd("viewed:" + token, timestamp, item);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item);
        }
        
        //执行那些被流水线包裹的命令
        pipe.exec();
    }
}
