package com.wsc.redisinaction.chapter04;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class PurchaseItem {
    public static final void main(String[] args) {
        new PurchaseItem().run();
    }

    public void run() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testPurchaseItem(conn);
    }

    public void testPurchaseItem(Jedis conn) {
        System.out.println("\n----- testPurchaseItem -----");
        testListItem(conn, true);

        System.out.println("We need to set up just enough state so a user can buy an item");
        conn.hset("users:userY", "funds", "125");
        Map<String,String> r = conn.hgetAll("users:userY");
        System.out.println("The user has some money:");
        for (Map.Entry<String,String> entry : r.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;
        assert r.get("funds") != null;
        System.out.println();

        System.out.println("Let's purchase an item");
        boolean p = purchaseItem(conn, "userY", "itemX", "userX", 10);
        System.out.println("Purchasing an item succeeded? " + p);
        assert p;
        r = conn.hgetAll("users:userY");
        System.out.println("Their money is now:");
        for (Map.Entry<String,String> entry : r.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;

        String buyer = "userY";
        Set<String> i = conn.smembers("inventory:" + buyer);
        System.out.println("Their inventory is now:");
        for (String member : i){
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        assert i.contains("itemX");
        assert conn.zscore("market:", "itemX.userX") == null;
    }
    
    
    /**
     * 测试？？
     * 
     * @param conn
     * @param nested
     */
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

    /**
     * 从市场上购买一件商品，支付与购买的过程
     * 
     * page 80
     * 代码清单 4-6
     * 
     * 
     * @param conn
     * @param buyerId
     * @param itemId
     * @param sellerId
     * @param lprice
     * @return
     */
    public boolean purchaseItem(
            Jedis conn, String buyerId, String itemId, String sellerId, double lprice) {

        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + '.' + sellerId;
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end){
        	
        	//对商品买卖市场以及买家的个人信息进行监控
            conn.watch("market:", buyer);

            //检查买家想要购买的商品的价格是否发生变化，以及买家是否有足够的钱来购买这件商品
            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));
            if (price != lprice || price > funds){
                conn.unwatch();
                return false;
            }

            //利用事务机制，先将买家支付的钱转移到卖家，然后将被购买的商品移交给买家
            Transaction trans = conn.multi();
            trans.hincrBy(seller, "funds", (int)price);
            trans.hincrBy(buyer, "funds", (int)-price);
            trans.sadd(inventory, itemId);
            trans.zrem("market:", item);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            //如果交易的过程中发生问题，那么交易重试，因为results为空代表了事务处理失败，返回了一个空值
            if (results == null){
                continue;
            }
            return true;
        }

        return false;
    }

}
