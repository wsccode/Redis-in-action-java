package com.wsc.redisinaction.chapter04;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class ListItem {
    public static final void main(String[] args) {
        new ListItem().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testListItem(conn, false);
    }

    public void testListItem(Jedis conn, boolean nested) {
        if (!nested){
            System.out.println("\n----- testListItem -----");
        }

        System.out.println("We need to set up just enough state so that a user can list an item");
        String seller = "userX";
        String item = "itemX";
        conn.sadd("inventory:" + seller, item);
        Set<String> i = conn.smembers("inventory:" + seller);

        System.out.println("The user's inventory has:");
        for (String member : i){
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        System.out.println();

        System.out.println("Listing the item...");
        boolean l = listItem(conn, item, seller, 10);
        System.out.println("Listing the item succeeded? " + l);
        assert l;
        Set<Tuple> r = conn.zrangeWithScores("market:", 0, -1);
        System.out.println("The market contains:");
        for (Tuple tuple : r){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert r.size() > 0;
    }
    
    
    
    /**
     * 
     * 监视，销售的商品
     * 
     * page 78
     * 代码清单 4-5
     * 
     * 
     * @param conn
     * @param itemId
     * @param sellerId
     * @param price
     * @return
     */
    public boolean listItem(
            Jedis conn, String itemId, String sellerId, double price) {

        String inventory = "inventory:" + sellerId;
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
        	//监视用户包裹发生的变化
            conn.watch(inventory);
            
            //检查用户是否仍然持有将要被销售的商品
            if (!conn.sismember(inventory, itemId)){
                //如果指定的商品不在用户包裹里面，那么停止对包裹键的监视并返回一个空值
            	conn.unwatch();
                return false;
            }

            //把被销售的商品添加到商品买卖市场里面
            Transaction trans = conn.multi();
            trans.zadd("market:", price, item);
            trans.srem(inventory, itemId);
            
            //如果执行exec方法没有引发watchError异常
            //那么说明事务已经执行成功，并且对包裹键的监视也结束
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                continue;
            }
            return true;
        }
        return false;
    }

}
