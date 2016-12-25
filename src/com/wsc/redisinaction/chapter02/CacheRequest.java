package com.wsc.redisinaction.chapter02;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


import redis.clients.jedis.Jedis;

public class CacheRequest {

	public static void main(String[] args) 
			throws InterruptedException
	{
		new CacheRequest().run();
	}
	
	public void run() 
			throws InterruptedException
	{	
		Jedis conn = new Jedis("localhost");
        conn.select(15);
        
        testCacheRequest(conn);
	}
	
	public void testCacheRequest(Jedis conn) {
        System.out.println("\n----- testCacheRequest -----");
        String token = UUID.randomUUID().toString();

        Callback callback = new Callback(){
            public String call(String request){
                return "content for " + request;
            }
        };

        updateToken(conn, token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("We are going to cache a simple request against " + url);
        String result = cacheRequest(conn, url, callback);
        System.out.println("We got initial content:\n" + result);
        System.out.println();

        assert result != null;

        System.out.println("To test that we've cached the request, we'll pass a bad callback");
        String result2 = cacheRequest(conn, url, null);
        System.out.println("We ended up getting the same response!\n" + result2);

        assert result.equals(result2);

        assert !canCache(conn, "http://test.com/");
        assert !canCache(conn, "http://test.com/?item=itemX&_=1234536");
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
     * 页面缓存函数
     * 
     * page 30
     *  清单 2-6
     * 
     * 
     * @param conn
     * @param request
     * @param callback
     * @return
     */
    public String cacheRequest(Jedis conn, String request, Callback callback) {
    	//对于不能直接被缓存的请求，直接调用回调函数
        if (!canCache(conn, request)){
            return callback != null ? callback.call(request) : null;
        }
        
        //将请求转换一个简单的字符串建，方便以后进行查找
        String pageKey = "cache:" + hashRequest(request);
        
        //尝试查找被缓存的页面
        String content = conn.get(pageKey);

        if (content == null && callback != null){
            //如果页面还没有被缓存，那么生成页面
        	content = callback.call(request);

        	//将新生成的页面放到缓存页面里
        	conn.setex(pageKey, 300, content);
        }

        return content;
    }

    /**
     * 是否需要被缓存
     * 
     * page 34
     * 清档 2-11
     * 
     * @param conn   Jedis
     * @param request 是否严缓存的页面
     * @return  
     */
    public boolean canCache(Jedis conn, String request) {
        try {
            URL url = new URL(request);
            HashMap<String,String> params = new HashMap<String,String>();
            if (url.getQuery() != null){
                for (String param : url.getQuery().split("&")){
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }

            //尝试长页面里面取出商品ID
            String itemId = extractItemId(params);
            
            //检查这个页面能否被缓存以及这个页面是否为商品页面
            if (itemId == null || isDynamic(params)) {
                return false;
            }
            
            //取得这个商品的浏览次数排名
            Long rank = conn.zrank("viewed:", itemId);
            
            //根据商品的浏览次数排名来判断是否需要缓存这个页面
            return rank != null && rank < 10000;
        }catch(MalformedURLException mue){
            return false;
        }
    }
    
    /**
     * 页面是否是商品的页面
     * 
     * 辅助函数
     * 
     * 
     * @param params
     * @return
     */
    public boolean isDynamic(Map<String,String> params) {
        return params.containsKey("_");
    }

    /**
     * 尝试长页面里面取出商品ID
     * 
     * 辅助函数
     * 
     * @param params
     * @return
     */
    public String extractItemId(Map<String,String> params) {
        return params.get("item");
    }

    
    /**
     * 将请求转化为一个hash码
     * 
     * 辅助函数
     * 
     * @param request
     * @return
     */
    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    /**
     * 回调函数
     * 
     * @author wsc
     *
     */
    public interface Callback {
        public String call(String request);
    }
}
