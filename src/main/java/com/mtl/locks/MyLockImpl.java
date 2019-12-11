package com.mtl.locks;

import com.mtl.zookeeper.ZKConnection;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 说明:分布式锁实现,只能用于单个线程（哪个线程创建，哪个线程使用），如需在多个线程使用，请在每个线程中创建新的Lock对象
 *      构造方法可传入一个key值，不同的key获取的锁对象不同，也就是说不同的key不会发生互斥
 *
 * @作者 莫天龙
 * @时间 2019/06/21 11:23
 */
public class MyLockImpl implements MyLock {
    private static Logger logger= LoggerFactory.getLogger(MyLockImpl.class);
    private static final String DEFAULT_NODE_NAME="COM_MTL_LOCKS_MYREENTRANTLOCK";
    private static final String NODE_NAME_PREFIX="/C_M_L_L_";
    private volatile int count=0;//用于如果在同一线程中多次使用锁，保证其可重入
    private String key;//锁标识，不同标识，不会生产互斥
    private String data;//保证谁获取锁，谁释放锁
    private int failed=0;//失败次数
    private Syn syn=new Syn();
    private Thread createThread=Thread.currentThread();//保证该锁只能在单线程中使用

    private ZooKeeper zookeeper;//获取锁的zookeeper连接，不同的key会有不同的zookeeper连接
    //同步器
    private static class Syn{
        String stat="0";
    }

    public MyLockImpl(String key) {
        this.key = key;
    }

    public MyLockImpl() {
        key=DEFAULT_NODE_NAME;
    }

    /**
     * 获取锁对象，改方法会阻塞直到获取到锁对象，如果获取锁对象过程中发生异常，程序会关闭获取该锁的连接，以保证释放已获取的锁,并重新尝试获取新的连接对象继续获取锁
     */
    @Override
    public void lock() {
        checkThread();
        //同一线程可以重入
        if (count>0){
            count++;
            return ;
        }
        zookeeper = ZKConnection.getZookeeper(key);
        data=UUID.randomUUID().toString();
        dolock();
    }

    /**
     * 释放已获取的锁对象，如果释放过程中发生异常，程序将关闭获取锁的连接来强制释放
     */
    @Override
    public void unlock() {
        checkThread();
        //同一线程可以重入
        if (count>1){
            count--;
            return;
        }
        Stat stat=new Stat();
        try {
            byte[] data = zookeeper.getData(NODE_NAME_PREFIX + key, false, stat);
            String lockData=new String(data, Charset.forName("UTF-8"));
            if (!lockData.equals(this.data)){
                throw new IllegalLockOwerException("lock creator exception!");
            }
            zookeeper.delete(NODE_NAME_PREFIX+key, stat.getVersion());
        }catch (IllegalLockOwerException ie){
            throw ie;
        }catch (Exception e){
            ZKConnection.close(key);
            logger.error("unlock failed!connection will closed!", e);
        }
    }


    /**
     * 检查创建锁的线程是否和使用锁的线程一致，保证锁在单线程中使用
     */
    private void checkThread(){
        if (Thread.currentThread()!=createThread){
            throw new RuntimeException("the lock's createThread is "+createThread+"!currentThread["+Thread.currentThread()+"] can not use it!");
        }
    }

    private void dolock(){
        try {
            Stat stat = zookeeper.exists(NODE_NAME_PREFIX + key, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    Event.EventType type = event.getType();
                    logger.info("the watcher triggered! type :{}", type);
                    synchronized (syn){
                        try {
                            if (Event.EventType.NodeDeleted==type){
                                String s = zookeeper.create(NODE_NAME_PREFIX + key, data.getBytes(Charset.forName("UTF-8")), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                syn.stat="1";
                                count=1;
                            }else{
                                syn.stat="2";
                            }
                        }catch (Exception e){
                            logger.error("watcher trigger exception!", e);
                            syn.stat="3";
                        }
                        syn.notifyAll();
                    }
                }
            });
            if (stat==null){
                String s = zookeeper.create(NODE_NAME_PREFIX + key, data.getBytes(Charset.forName("UTF-8")), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.debug("create node {} successful!",s);
                count=1;
                return ;
            }else{
                synchronized (syn){
                    while ("0".equals(syn.stat)) {
                        logger.debug("wait other system unlock...");
                        syn.wait();
                    }
                }
                logger.debug("get thread weak up ! stat :{}",syn.stat);
                if ("1".equals(syn.stat)){//创建成功
                    return ;
                }else{//其他状态  创建没成功
                    syn.stat="0";
                    dolock();
                }
            }
        } catch (Exception e) {
            if (e instanceof KeeperException.NodeExistsException){
                if (logger.isDebugEnabled()){
                    logger.debug("get lock failed!will try again!");
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                }catch (InterruptedException ie){
                }
                syn.stat="0";
                dolock();
            }else {
                ZKConnection.close(key);
                if (failed>=1000){
                    throw new RuntimeException("get lock failed!", e);
                }else{
                    if (logger.isDebugEnabled()){
                        logger.debug("connect failed!will try again!try times:"+failed);
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(20);
                    }catch (InterruptedException ie){}
                    zookeeper=ZKConnection.getZookeeper(key);
                    syn.stat="0";
                    dolock();
                }
            }
        }
    }
}
