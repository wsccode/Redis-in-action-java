package com.wsc.redisinaction.chapter06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class AddUpdateContact {
    public static final void main(String[] args)
    	throws Exception
    {
        new AddUpdateContact().run();
    }
    public void run()
        throws InterruptedException, IOException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testAddUpdateContact(conn);
        
    }
    public void testAddUpdateContact(Jedis conn) {
        System.out.println("\n----- testAddUpdateContact -----");
        conn.del("recent:user");

        System.out.println("Let's add a few contacts...");
        for (int i = 0; i < 10; i++){
            addUpdateContact(conn, "user", "contact-" + ((int)Math.floor(i / 3)) + '-' + i);
        }
        System.out.println("Current recently contacted contacts");
        List<String> contacts = conn.lrange("recent:user", 0, -1);
        for(String contact : contacts){
            System.out.println("  " + contact);
        }
        assert contacts.size() >= 10;
        System.out.println();

        System.out.println("Let's pull one of the older ones up to the front");
        addUpdateContact(conn, "user", "contact-1-4");
        contacts = conn.lrange("recent:user", 0, 2);
        System.out.println("New top-3 contacts:");
        for(String contact : contacts){
            System.out.println("  " + contact);
        }
        assert "contact-1-4".equals(contacts.get(0));
        System.out.println();

        System.out.println("Let's remove a contact...");
        removeContact(conn, "user", "contact-2-6");
        contacts = conn.lrange("recent:user", 0, -1);
        System.out.println("New contacts:");
        for(String contact : contacts){
            System.out.println("  " + contact);
        }
        assert contacts.size() >= 9;
        System.out.println();

        System.out.println("And let's finally autocomplete on ");
        List<String> all = conn.lrange("recent:user", 0, -1);
        contacts = fetchAutocompleteList(conn, "user", "c");
        assert all.equals(contacts);
        List<String> equiv = new ArrayList<String>();
        for (String contact : all){
            if (contact.startsWith("contact-2-")){
                equiv.add(contact);
            }
        }
        contacts = fetchAutocompleteList(conn, "user", "contact-2-");
        Collections.sort(equiv);
        Collections.sort(contacts);
        assert equiv.equals(contacts);
        conn.del("recent:user");
    }

    /**
     * 添加新的用户并更新最新的列表
     * 
     * page 111
     * 代码清单 6-1
     * 
     * @param conn 数据库连接
     * @param user 用户
     * @param contact 要添加的用户
     */
    public void addUpdateContact(Jedis conn, String user, String contact) {
        String acList = "recent:" + user;
        
        //利用事务准备执行原子操作
        Transaction trans = conn.multi();
        //如果联系人已经存在，那么移除他
        trans.lrem(acList, 0, contact);
        //将联系人推入列表的最前端
        trans.lpush(acList, contact);
        //只保留列表里面的前100个联系人
        trans.ltrim(acList, 0, 99);
        //利用事务执行上面的操作
        trans.exec();
    }

    /**
     * 将联系人从用户的列表中删除
     * 
     * page 111
     * 
     * 
     * @param conn	数据库连接
     * @param user	用户名
     * @param contact	联系人
     */
    public void removeContact(Jedis conn, String user, String contact) {
        conn.lrem("recent:" + user, 0, contact);
    }

    /**
     * 获取自动补全列表并查找匹配的用户
     * 
     * page 111
     * 清单6-2
     * 
     * @param conn
     * @param user
     * @param prefix
     * @return
     */
    public List<String> fetchAutocompleteList(Jedis conn, String user, String prefix) {
    	//获取自动补全的列表
        List<String> candidates = conn.lrange("recent:" + user, 0, -1);
        List<String> matches = new ArrayList<String>();
        //检查每个候选联系人
        for (String candidate : candidates) {
            if (candidate.toLowerCase().startsWith(prefix)){
                //发现一个匹配的联系人
            	matches.add(candidate);
            }
        }
        //返回所有匹配的联系人
        return matches;
    }

    //准备一个由已知字符组成的列表
    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";
    /**
     * 根据给定前缀生成查找范围
     * 
     * page 113
     * 清单6-3
     * 
     * @param prefix
     * @return
     */
    public String[] findPrefixRange(String prefix) {
        //在字符列表中查找前缀字符所在的位置
    	int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        //找到前驱字符
    	char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        
    	String start = prefix.substring(0, prefix.length() - 1) + suffix + '{';
        String end = prefix + '{';
        //返回范围
        return new String[]{start, end};
    }

    /**
     * 加入公会
     * 
     * page 115
     * 清单6-5
     * 
     * @param conn
     * @param guild
     * @param user
     */
    public void joinGuild(Jedis conn, String guild, String user) {
        conn.zadd("members:" + guild, 0, user);
    }

    /**
     * 离开公会
     * 
     * page 115
     * 清档6-5
     * 
     * @param conn
     * @param guild
     * @param user
     */
    public void leaveGuild(Jedis conn, String guild, String user) {
        conn.zrem("members:" + guild, user);
    }

    /**
     * 自动补全方法，通过使用watch multi exec 确保集合债进行查找是不会发生变化
     * 
     * page 114
     * 清单6-4
     * 
     * @param conn
     * @param guild
     * @param prefix
     * @return
     */
    @SuppressWarnings("unchecked")
    public Set<String> autocompleteOnPrefix(Jedis conn, String guild, String prefix) {
    	//根据给定的前缀计算出查找范围的起点和终点
        String[] range = findPrefixRange(prefix);
        String start = range[0];
        String end = range[1];
        String identifier = UUID.randomUUID().toString();
        start += identifier;
        end += identifier;
        String zsetName = "members:" + guild;

        //将范围的起点元素和结束元素添加到有序集合里面
        conn.zadd(zsetName, 0, start);
        conn.zadd(zsetName, 0, end);

        Set<String> items = null;
        while (true){
            conn.watch(zsetName);
            
            //找到两个被插入元素在有序集合的排名
            int sindex = conn.zrank(zsetName, start).intValue();
            int eindex = conn.zrank(zsetName, end).intValue();
            int erange = Math.min(sindex + 9, eindex - 2);

            //创建事务
            Transaction trans = conn.multi();
            
            //获取范围内的值，然后删除之前插入的起始元素和结束元素
            trans.zrem(zsetName, start);
            trans.zrem(zsetName, end);
            trans.zrange(zsetName, sindex, erange);
            List<Object> results = trans.exec();
            if (results != null){
                items = (Set<String>)results.get(results.size() - 1);
                break;
            }
        }
        
        
        //如果正在有其他的自动补全的操作正在执行，
        //那么从获取到的元素里面移除起始元素和结束元素
        for (Iterator<String> iterator = items.iterator(); iterator.hasNext(); ){
            if (iterator.next().indexOf('{') != -1){
                iterator.remove();
            }
        }
        return items;
    }
    
    
}
