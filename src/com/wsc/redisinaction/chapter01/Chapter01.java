package com.wsc.redisinaction.chapter01;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 {
	
	//准备好需要的常量
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400; //每周的秒数
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static  void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
    	
    	//用于连接redis数据库的类，默认本机的数据库
        Jedis conn = new Jedis("localhost");
        	
        conn.select(15);
        
        //发布新文章
        String articleId = postArticle(
            conn, "username", "A title", "http://www.google.com");
        System.out.println("  We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String,String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String,String> entry : articleData.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();

        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println(" We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        System.out.println("The currently highest-scoring articles are:");
        List<Map<String,String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;
        
        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 发布新文章
     * 
     * page 18
     * 代码清单 1-7
     * 
     * @param conn
     * @param user
     * @param title
     * @param link
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
    	
    	//生成一个新的文章的ID
        String articleId = String.valueOf(conn.incr("article:"));
        
        String voted = "voted:" + articleId;
        
        //将发布的文章的用户添加到文章的已投票用户名单中
        conn.sadd(voted, user);
        
        //将这个已投票的名单的过期时间设置为一周
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        //利用java实现一个map，将文章信息存储在这个map里面
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String,String> articleData = new HashMap<String,String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        
        //将上面的map存储到一个散列里面
        conn.hmset(article, articleData);
        
        //将文章添加到根据发布时间排序的有序集合里面
        conn.zadd("score:", now + VOTE_SCORE, article);
        
        //将文章添加到根据评分排序的有序集合
        conn.zadd("time:", now, article);

        return articleId;
    }

    
    /**
     * 投票功能的实现函数
     * 
     * page 17 
     * 代码清单1-6
     * 
     * @param conn
     * @param user
     * @param article
     */
    public void articleVote(Jedis conn, String user, String article) {
    	//计算文章投票的截止时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        
        //检查文章是否还可以继续投票
        if (conn.zscore("time:", article) < cutoff){
            return;
        }
        
        //从article:Id标识符（identifier）里面取出文章的id
        String articleId = article.substring(article.indexOf(':') + 1);
        
        //如果用户是第一次为此篇文章投票，那么增加这篇文章的投票数和分数
        if (conn.sadd("voted:" + articleId, user) == 1) {
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1l);
        }
    }

    //下面函数的辅助函数
    //这种模式我也不太清楚叫什么模式，求教！
    public List<Map<String,String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    
    /**
     * 获取文章
     * page 18 
     * 
     * 清单 1-8
     * 
     * @param conn
     * @param page
     * @param order
     * @return 返回获取文章list
     */
    public List<Map<String,String>> getArticles(Jedis conn, int page, String order) {
    	
    	//获取文章的起始索引和结束索引
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        //获取读个文章
        Set<String> ids = conn.zrevrange(order, start, end);
        
        //根据文章的ID获取文章的详细信息
        List<Map<String,String>> articles = new ArrayList<Map<String,String>>();
        for (String id : ids){
            Map<String,String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 文章添加到群组
     * 
     * page 19
     * 清单 1-9
     * 
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
    	
    	//构建存储文章信息的键名
        String article = "article:" + articleId;
        
        //将文章添加到它所属的群组里面
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    //同上
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    /**
     * 从群组中获取一整页文章
     * 
     * page 20
     * 清单1-10
     * 
     * @param conn
     * @param group
     * @param page
     * @param order
     * @return
     */
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page, String order) {
    	
    	//为每个群组的每种排序都建立一个键
        String key = order + group;
        
        //检查是否有缓存的排序结果
        //如果没有的话就现在进行排序
        if (!conn.exists(key)) {
            //根据评分或者发布时间，对群组文章进行排序
        	ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            
            //让redis在60秒之后自动的删除这个有序的集合
            conn.expire(key, 60);
        }
        
        //在返回的内容中，调用之前定义的函数来进行分页并获取文章数据
        return getArticles(conn, page, key);
    }

    /**
     * 辅助函数，在redisinaaction中没有，是源码作业为了方便打印自己加上的
     * 
     * 打印链表中文章的所有信息
     * 
     * @param articles
     */
    private void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}

