package com.wsc.redisinaction.chapter06;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Chapter06 {
    public static final void main(String[] args)
        throws Exception
    {
        new Chapter06().run();
    }

    public void run()
        throws InterruptedException, IOException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        testAddUpdateContact(conn);
        testAddressBookAutocomplete(conn);
        testDistributedLocking(conn);
        testCountingSemaphore(conn);
        testDelayedTasks(conn);
        testMultiRecipientMessaging(conn);
        testFileDistribution(conn);
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

    public void testDelayedTasks(Jedis conn)
        throws InterruptedException
    {
        System.out.println("\n----- testDelayedTasks -----");
        conn.del("queue:tqueue", "delayed:");
        System.out.println("Let's start some regular and delayed tasks...");
        for (long delay : new long[]{0, 500, 0, 1500}){
            assert executeLater(conn, "tqueue", "testfn", new ArrayList<String>(), delay) != null;
        }
        long r = conn.llen("queue:tqueue");
        System.out.println("How many non-delayed tasks are there (should be 2)? " + r);
        assert r == 2;
        System.out.println();

        System.out.println("Let's start up a thread to bring those delayed tasks back...");
        PollQueueThread thread = new PollQueueThread();
        thread.start();
        System.out.println("Started.");
        System.out.println("Let's wait for those tasks to be prepared...");
        Thread.sleep(2000);
        thread.quit();
        thread.join();
        r = conn.llen("queue:tqueue");
        System.out.println("Waiting is over, how many tasks do we have (should be 4)? " + r);
        assert r == 4;
        conn.del("queue:tqueue", "delayed:");
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

    public void testFileDistribution(Jedis conn)
        throws InterruptedException, IOException
    {
        System.out.println("\n----- testFileDistribution -----");
        String[] keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0){
            conn.del(keys);
        }
        conn.del(
            "msgs:test:",
            "seen:0",
            "seen:source",
            "ids:test:",
            "chat:test:");

        System.out.println("Creating some temporary 'log' files...");
        File f1 = File.createTempFile("temp_redis_1_", ".txt");
        f1.deleteOnExit();
        Writer writer = new FileWriter(f1);
        writer.write("one line\n");
        writer.close();

        File f2 = File.createTempFile("temp_redis_2_", ".txt");
        f2.deleteOnExit();
        writer = new FileWriter(f2);
        for (int i = 0; i < 100; i++){
            writer.write("many lines " + i + '\n');
        }
        writer.close();

        File f3 = File.createTempFile("temp_redis_3_", ".txt.gz");
        f3.deleteOnExit();
        writer = new OutputStreamWriter(
            new GZIPOutputStream(
                new FileOutputStream(f3)));
        Random random = new Random();
        for (int i = 0; i < 1000; i++){
            writer.write("random line " + Long.toHexString(random.nextLong()) + '\n');
        }
        writer.close();

        long size = f3.length();
        System.out.println("Done.");
        System.out.println();
        System.out.println("Starting up a thread to copy logs to redis...");
        File path = f1.getParentFile();
        CopyLogsThread thread = new CopyLogsThread(path, "test:", 1, size);
        thread.start();

        System.out.println("Let's pause to let some logs get copied to Redis...");
        Thread.sleep(250);
        System.out.println();
        System.out.println("Okay, the logs should be ready. Let's process them!");

        System.out.println("Files should have 1, 100, and 1000 lines");
        TestCallback callback = new TestCallback();
        processLogsFromRedis(conn, "0", callback);
        System.out.println(Arrays.toString(callback.counts.toArray(new Integer[0])));
        assert callback.counts.get(0) == 1;
        assert callback.counts.get(1) == 100;
        assert callback.counts.get(2) == 1000;

        System.out.println();
        System.out.println("Let's wait for the copy thread to finish cleaning up...");
        thread.join();
        System.out.println("Done cleaning out Redis!");

        keys = conn.keys("test:*").toArray(new String[0]);
        if (keys.length > 0){
            conn.del(keys);
        }
        conn.del(
            "msgs:test:",
            "seen:0",
            "seen:source",
            "ids:test:",
            "chat:test:");
    }

    public class TestCallback
        implements Callback
    {
        private int index;
        public List<Integer> counts = new ArrayList<Integer>();

        public void callback(String line){
            if (line == null){
                index++;
                return;
            }
            while (counts.size() == index){
                counts.add(0);
            }
            counts.set(index, counts.get(index) + 1);
        }
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

    /**
     * 创建延迟任务
     * 
     * page 137
     * 
     * 清单 6-22
     * 
     * @param conn
     * @param queue
     * @param name
     * @param args
     * @param delay
     * @return
     */
    public String executeLater(
        Jedis conn, String queue, String name, List<String> args, long delay)
    {
    	//Josn列表
        Gson gson = new Gson();
        
        //生成唯一的标识符
        String identifier = UUID.randomUUID().toString();
        
        //准备好需要入队的任务
        String itemArgs = gson.toJson(args);
        String item = gson.toJson(new String[]{identifier, queue, name, itemArgs});
        
        if (delay > 0){
        	//延迟这个任务的执行时间
            conn.zadd("delayed:", System.currentTimeMillis() + delay, item);
        } else {
        	//立即执行这个任务
        	conn.rpush("queue:" + queue, item);
        }
        //返回标识符
        return identifier;
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

    /**
     * 接收日志文件 
     * 
     * page 149
     * 
     * 
     * @param conn
     * @param id
     * @param callback
     * @throws InterruptedException
     * @throws IOException
     */
    public void processLogsFromRedis(Jedis conn, String id, Callback callback)
        throws InterruptedException, IOException
    {
        while (true){
        	//获取文件列表
            List<ChatMessages> fdata = fetchPendingMessages(conn, id);

            for (ChatMessages messages : fdata){
                for (Map<String,Object> message : messages.messages){
                    String logFile = (String)message.get("message");
                    
                    //所有日志行已经处理完毕
                    if (":done".equals(logFile)){
                        return;
                    }
                    if (logFile == null || logFile.length() == 0){
                        continue;
                    }

                    //选择一个块读取器
                    InputStream in = new RedisInputStream(
                        conn, messages.chatId + logFile);
                    if (logFile.endsWith(".gz")){
                        in = new GZIPInputStream(in);
                    }

                    //遍历日志行
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    try{
                        String line = null;
                        while ((line = reader.readLine()) != null){
                            //将日志行传递给回调函数
                        	callback.callback(line);
                        }
                        //强制刷新聚合数据缓存
                        callback.callback(null);
                    }finally{
                        reader.close();
                    }

                    //日志已经处理完毕，向文件发送者报告这一信息
                    conn.incr(messages.chatId + logFile + ":done");
                }
            }

            if (fdata.size() == 0){
                Thread.sleep(100);
            }
        }
    }

    /**
     * 块读取器，从rides内存中读取信息
     * 
     * page 150以后
     * 
     * @author wsc
     *
     */
    public class RedisInputStream
        extends InputStream
    {
        private Jedis conn;
        private String key;
        private int pos;

        public RedisInputStream(Jedis conn, String key){
            this.conn = conn;
            this.key = key;
        }

        @Override
        public int available()
            throws IOException
        {
            long len = conn.strlen(key);
            return (int)(len - pos);
        }

        @Override
        public int read()
            throws IOException
        {
            byte[] block = conn.substr(key.getBytes(), pos, pos);
            if (block == null || block.length == 0){
                return -1;
            }
            pos++;
            return (int)(block[0] & 0xff);
        }

        @Override
        public int read(byte[] buf, int off, int len)
            throws IOException
        {
            byte[] block = conn.substr(key.getBytes(), pos, pos + (len - off - 1));
            if (block == null || block.length == 0){
                return -1;
            }
            System.arraycopy(block, 0, buf, off, block.length);
            pos += block.length;
            return block.length;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    public interface Callback {
        void callback(String line);
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

    /**
     * 从延迟队列中获取可执行任务
     * 
     * page 138
     * 
     * 
     * @author wsc
     *
     */
    public class PollQueueThread
        extends Thread
    {
        private Jedis conn;
        private boolean quit;
        private Gson gson = new Gson();

        public PollQueueThread(){
            this.conn = new Jedis("localhost");
            this.conn.select(15);
        }

        public void quit() {
            quit = true;
        }

        public void run() {
            while (!quit){
            	//获取任务队列中的第一个任务
                Set<Tuple> items = conn.zrangeWithScores("delayed:", 0, 0);
                Tuple item = items.size() > 0 ? items.iterator().next() : null;
                
                //队列中没有包含任何任务，或者任务的执行时间未到
                if (item == null || item.getScore() > System.currentTimeMillis()) {
                    try{
                        sleep(10);
                    }catch(InterruptedException ie){
                        Thread.interrupted();
                    }
                    continue;
                }
                
                //解码要被执行的任务，弄清楚他应该被推入哪个任务的队列
                String json = item.getElement();
                String[] values = gson.fromJson(json, String[].class);
                String identifier = values[0];
                String queue = values[1];
                
                //获取锁失败，跳过下面的步骤并重试
                String locked = acquireLock(conn, identifier);
                if (locked == null){
                    continue;
                }
                
                //将任务推入适当的任务队列里面
                if (conn.zrem("delayed:", json) == 1){
                    conn.rpush("queue:" + queue, json);
                }
                
                //释放锁
                releaseLock(conn, identifier, locked);
            }
        }
    }

    /**
     * 发送日志文件，将日志文件发到redis中
     * 
     * 
     * @author wsc
     *
     */
    public class CopyLogsThread
        extends Thread
    {
        private Jedis conn;
        private File path;
        private String channel;
        private int count;
        private long limit;

        public CopyLogsThread(File path, String channel, int count, long limit) {
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.path = path;
            this.channel = channel;
            this.count = count;
            this.limit = limit;
        }

        public void run() {
            Deque<File> waiting = new ArrayDeque<File>();
            long bytesInRedis = 0;

            Set<String> recipients= new HashSet<String>();
            for (int i = 0; i < count; i++){
                recipients.add(String.valueOf(i));
            }
            
            //创建用于向客户端发送消息的群组
            createChat(conn, "source", recipients, "", channel);
            File[] logFiles = path.listFiles(new FilenameFilter(){
                public boolean accept(File dir, String name){
                    return name.startsWith("temp_redis");
                }
            });
            Arrays.sort(logFiles);
            //遍历所有的日志文件
            for (File logFile : logFiles){
                long fsize = logFile.length();
                
                //如果程序需要更多的控件，那么清理已经处理完毕的文件
                while ((bytesInRedis + fsize) > limit){
                    long cleaned = clean(waiting, count);
                    if (cleaned != 0){
                        bytesInRedis -= cleaned;
                    }else{
                        try{
                            sleep(250);
                        }catch(InterruptedException ie){
                            Thread.interrupted();
                        }
                    }
                }

                BufferedInputStream in = null;
                try{
                    in = new BufferedInputStream(new FileInputStream(logFile));
                    int read = 0;
                    byte[] buffer = new byte[8192];
                    
                    //将文件上传至redis
                    while ((read = in.read(buffer, 0, buffer.length)) != -1){
                        if (buffer.length != read){
                            byte[] bytes = new byte[read];
                            System.arraycopy(buffer, 0, bytes, 0, read);
                            conn.append((channel + logFile).getBytes(), bytes);
                        }else{
                            conn.append((channel + logFile).getBytes(), buffer);
                        }
                    }
                }catch(IOException ioe){
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }finally{
                    try{
                        in.close();
                    }catch(Exception ignore){
                    }
                }
                //提醒监听着，文件已经准备就绪
                sendMessage(conn, channel, "source", logFile.toString());

                //对本地记录的redis内存占用量相关的信息进行更新
                bytesInRedis += fsize;
                waiting.addLast(logFile);
            }
            
            //所有日志文件已经处理完毕，向监听者报告此事
            sendMessage(conn, channel, "source", ":done");

            //在工作完成之后，清理无用的日志文件
            while (waiting.size() > 0){
                long cleaned = clean(waiting, count);
                if (cleaned != 0){
                    bytesInRedis -= cleaned;
                }else{
                    try{
                        sleep(250);
                    }catch(InterruptedException ie){
                        Thread.interrupted();
                    }
                }
            }
        }
        
        //对redis进行清理的详细步骤
        private long clean(Deque<File> waiting, int count) {
            if (waiting.size() == 0){
                return 0;
            }
            File w0 = waiting.getFirst();
            if (String.valueOf(count).equals(conn.get(channel + w0 + ":done"))){
                conn.del(channel + w0, channel + w0 + ":done");
                return waiting.removeFirst().length();
            }
            return 0;
        }
    }
}
