package com.wsc.redisinaction.chapter06;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class MultiRecipientMessaging {
    public static final void main(String[] args)
            throws Exception
    {
            new MultiRecipientMessaging().run();
    }

        public void run()
            throws InterruptedException, IOException
    {
            Jedis conn = new Jedis("localhost");
            conn.select(15);

            testMultiRecipientMessaging(conn);
    }
    
        public void testMultiRecipientMessaging(Jedis conn) {
            System.out.println("\n----- testMultiRecipientMessaging -----");
            conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");

            System.out.println("Let's create a new chat session with some recipients...");
            Set<String> recipients = new HashSet<String>();
            recipients.add("jeff");
            recipients.add("jenny");
            String chatId = createChat(conn, "joe", recipients, "message 1");
            System.out.println("Now let's send a few messages...");
            for (int i = 2; i < 5; i++){
                sendMessage(conn, chatId, "joe", "message " + i);
            }
            System.out.println();

            System.out.println("And let's get the messages that are waiting for jeff and jenny...");
            List<ChatMessages> r1 = fetchPendingMessages(conn, "jeff");
            List<ChatMessages> r2 = fetchPendingMessages(conn, "jenny");
            System.out.println("They are the same? " + r1.equals(r2));
            assert r1.equals(r2);
            System.out.println("Those messages are:");
            for(ChatMessages chat : r1){
                System.out.println("  chatId: " + chat.chatId);
                System.out.println("    messages:");
                for(Map<String,Object> message : chat.messages){
                    System.out.println("      " + message);
                }
            }

            conn.del("ids:chat:", "msgs:1", "ids:1", "seen:joe", "seen:jeff", "seen:jenny");
        }
        /**
         * 产生一个新的群组ID然后传给下一个函数
         * 
         * @param conn
         * @param sender
         * @param recipients
         * @param message
         * @return
         */
        public String createChat(Jedis conn, String sender, Set<String> recipients, String message) {
        	//获取新的群组ID
            String chatId = String.valueOf(conn.incr("ids:chat:"));
            return createChat(conn, sender, recipients, message, chatId);
        }

        /**
         * 
         * 创建群组聊天
         * 
         * page 142 
         * 清单 6-24
         * 
         * @param conn
         * @param sender
         * @param recipients
         * @param message
         * @param chatId
         * @return
         */
        public String createChat(
            Jedis conn, String sender, Set<String> recipients, String message, String chatId)
        {
        	//创建一个由用户和分组组成的字典，字典里面的信息将被添加到有序集合里面
            recipients.add(sender);

            Transaction trans = conn.multi();
            for (String recipient : recipients){
            	//将所有参与群聊的用户添加到有序集合里面
                trans.zadd("chat:" + chatId, 0, recipient);
                
                //初始化已读有序集合
                trans.zadd("seen:" + recipient, 0, chatId);
            }
            //执行事务
            trans.exec();

            //发送消息，调用sendMessage函数
            return sendMessage(conn, chatId, sender, message);
        }

        /**
         * 发送消息， 使用锁来实现的消息发送操作
         * 
         *page 143
         *清单6-25
         * 
         * @param conn
         * @param chatId
         * @param sender
         * @param message
         * @return
         */
        public String sendMessage(Jedis conn, String chatId, String sender, String message) {
        	
        	//获取锁
            String identifier = acquireLock(conn, "chat:" + chatId);
            if (identifier == null){
                throw new RuntimeException("Couldn't get the lock");
            }
            try {
            	//筹备待发送的，
                long messageId = conn.incr("ids:" + chatId);
                
                //将发送的消息先存入到hashmap中
                HashMap<String,Object> values = new HashMap<String,Object>();
                values.put("id", messageId);
                values.put("ts", System.currentTimeMillis());
                values.put("sender", sender);
                values.put("message", message);
                
                //然后将hashmap转为json格式的字符串
                String packed = new Gson().toJson(values);
                
                //将消息发送至群组
                conn.zadd("msgs:" + chatId, messageId, packed);
            }finally{
            	//最后释放锁
                releaseLock(conn, "chat:" + chatId, identifier);
            }
            return chatId;
        }

        /**
         * ！！！！！！！！1看不懂，根本没法翻译
         * 
         * page 144
         * 
         * 
         * @param conn
         * @param recipient
         * @return
         */
        @SuppressWarnings("unchecked")
        public List<ChatMessages> fetchPendingMessages(Jedis conn, String recipient) {
        	
        	//获取最后接收到信息的ID
            Set<Tuple> seenSet = conn.zrangeWithScores("seen:" + recipient, 0, -1);
            List<Tuple> seenList = new ArrayList<Tuple>(seenSet);

            //获取所有未读消息
            Transaction trans = conn.multi();
            for (Tuple tuple : seenList){
                String chatId = tuple.getElement();
                int seenId = (int)tuple.getScore();
                trans.zrangeByScore("msgs:" + chatId, String.valueOf(seenId + 1), "inf");
            }
            List<Object> results = trans.exec();

            Gson gson = new Gson();
            Iterator<Tuple> seenIterator = seenList.iterator();
            Iterator<Object> resultsIterator = results.iterator();

            List<ChatMessages> chatMessages = new ArrayList<ChatMessages>();
            List<Object[]> seenUpdates = new ArrayList<Object[]>();
            List<Object[]> msgRemoves = new ArrayList<Object[]>();
            while (seenIterator.hasNext()){
                Tuple seen = seenIterator.next();
                Set<String> messageStrings = (Set<String>)resultsIterator.next();
                if (messageStrings.size() == 0){
                    continue;
                }

                int seenId = 0;
                String chatId = seen.getElement();
                List<Map<String,Object>> messages = new ArrayList<Map<String,Object>>();
                for (String messageJson : messageStrings){
                    Map<String,Object> message = (Map<String,Object>)gson.fromJson(
                        messageJson, new TypeToken<Map<String,Object>>(){}.getType());
                    int messageId = ((Double)message.get("id")).intValue();
                    if (messageId > seenId){
                        seenId = messageId;
                    }
                    message.put("id", messageId);
                    messages.add(message);
                }

                conn.zadd("chat:" + chatId, seenId, recipient);
                seenUpdates.add(new Object[]{"seen:" + recipient, seenId, chatId});

                Set<Tuple> minIdSet = conn.zrangeWithScores("chat:" + chatId, 0, 0);
                if (minIdSet.size() > 0){
                    msgRemoves.add(new Object[]{
                        "msgs:" + chatId, minIdSet.iterator().next().getScore()});
                }
                chatMessages.add(new ChatMessages(chatId, messages));
            }

            trans = conn.multi();
            for (Object[] seenUpdate : seenUpdates){
                trans.zadd(
                    (String)seenUpdate[0],
                    (Integer)seenUpdate[1],
                    (String)seenUpdate[2]);
            }
            for (Object[] msgRemove : msgRemoves){
                trans.zremrangeByScore(
                    (String)msgRemove[0], 0, ((Double)msgRemove[1]).intValue());
            }
            trans.exec();

            return chatMessages;
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
         * 内部类定义一些内容模糊与操作
         * 
         * @author wsc
         *
         */
        public class ChatMessages
        {
            public String chatId;
            public List<Map<String,Object>> messages;

            public ChatMessages(String chatId, List<Map<String,Object>> messages){
                this.chatId = chatId;
                this.messages = messages;
            }

            public boolean equals(Object other){
                if (!(other instanceof ChatMessages)){
                    return false;
                }
                ChatMessages otherCm = (ChatMessages)other;
                return chatId.equals(otherCm.chatId) &&
                    messages.equals(otherCm.messages);
            }
        }
}
