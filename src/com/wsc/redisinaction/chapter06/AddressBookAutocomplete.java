package com.wsc.redisinaction.chapter06;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class AddressBookAutocomplete {

	public static void main(String[] args) 
		throws Exception
	{
	        new AddressBookAutocomplete().run();
	}
    public void run()
            throws InterruptedException, IOException
    {
            Jedis conn = new Jedis("localhost");
            conn.select(15);
            
            testAddressBookAutocomplete(conn);   
    }
    /**
     * 通讯录自动补全
     * 
     * 
     * @param conn
     */
    public void testAddressBookAutocomplete(Jedis conn) {
        System.out.println("\n----- testAddressBookAutocomplete -----");
        conn.del("members:test");
        System.out.println("the start/end range of 'abc' is: " +
            Arrays.toString(findPrefixRange("abc")));
        System.out.println();

        System.out.println("Let's add a few people to the guild");
        for (String name : new String[]{"jeff", "jenny", "jack", "jennifer"}){
            joinGuild(conn, "test", name);
        }
        System.out.println();
        System.out.println("now let's try to find users with names starting with 'je':");
        Set<String> r = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(r);
        assert r.size() == 3;

        System.out.println("jeff just left to join a different guild...");
        leaveGuild(conn, "test", "jeff");
        r = autocompleteOnPrefix(conn, "test", "je");
        System.out.println(r);
        assert r.size() == 2;
        conn.del("members:test");
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
