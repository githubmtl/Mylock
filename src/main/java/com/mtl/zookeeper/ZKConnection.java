package com.mtl.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 说明:ZK连接
 *
 * @作者 莫天龙
 * @时间 2019/06/21 11:59
 */
public class ZKConnection {


    private static final String DEFAULT_URL="127.0.0.1:2181";
    private static final String URL_KEY="lock.zookeeper.url";

    private static Logger logger= LoggerFactory.getLogger(ZKConnection.class);

    private static volatile String url;

    private static Map<String,ZooKeeper> zooKeeperMap=new HashMap<>();

    /**
     * 不同的key，获取不同zk连接，当连接断掉后，可自动解锁
     * @param key
     * @return
     */
    public synchronized static ZooKeeper getZookeeper(String key){
        if (url==null){
            url=getUrl();
        }
        if (zooKeeperMap.containsKey(key)){
            return zooKeeperMap.get(key);
        }
        ZooKeeper zooKeeper = init(url);
        zooKeeperMap.put(key, zooKeeper);
        return zooKeeper;
    }

    /**
     * 连接zookeeper
     * @param url
     * @return
     */
    private static ZooKeeper init(String url){
        CountDownLatch countDownLatch=new CountDownLatch(1);
        ZooKeeper zooKeeper=null;
        try {
            zooKeeper=new ZooKeeper(url, 30000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType()== Event.EventType.None&&event.getState()== Event.KeeperState.SyncConnected){
                        countDownLatch.countDown();
                        if (logger.isDebugEnabled()){
                            logger.debug("trigger connected! ");
                        }
                    }
                }
            });
            countDownLatch.await();
            if (logger.isDebugEnabled()){
                logger.debug("connect zookeeper successful!");
            }
        }catch (IOException e){
            logger.error("connect to zookeeper failed!", e);
            throw new RuntimeException("connect to zookeeper failed!", e);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }
        return zooKeeper;
    }

    /**
     * 获取连接配置信息
     * @return
     */
    private static String getUrl(){
        String url=null;
        url=System.getProperty(URL_KEY);
        if (url!=null&&!url.trim().equals("")){
            return url;
        }
        try {
            Properties properties=new Properties();
            properties.load(ZKConnection.class.getResourceAsStream("/mylock.properties"));
            url = properties.getProperty(URL_KEY);
            if (url!=null&&!url.trim().equals("")){
                return url;
            }
        }catch (IOException e){
            logger.error("system error!",e);
        }
        return DEFAULT_URL;
    }

    public synchronized static void removeZookeeper(String key){
        zooKeeperMap.remove(key);
    }

    public static void close(String key){
        ZooKeeper zooKeeper = zooKeeperMap.get(key);
        if (zooKeeper!=null){
            removeZookeeper(key);
            try {
                zooKeeper.close();
            }catch (InterruptedException e){
                logger.error("system error!",e);
            }
        }
    }

}
