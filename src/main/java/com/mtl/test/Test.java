package com.mtl.test;

import com.mtl.locks.MyLock;
import com.mtl.locks.MyLockImpl;

import java.util.concurrent.TimeUnit;

/**
 * 说明：测试类
 *
 * @author 莫天龙
 * @email 92317919@qq.com
 * @dateTime 2019/10/11 20:44
 */
public class Test {
    public static void main(String[] args) throws Exception {
        //通过系统参数设置zookeeper连接参数，也可以在目录路径下创建mylock.properties文件，默认连接地址为：127.0.0.1:2181
        System.setProperty("lock.zookeeper.url","127.0.0.1:2181");
        for (int i=0;i<5;i++){
            new Thread(){
                @Override
                public void run() {
                    String name = Thread.currentThread().getName();
                    MyLock lock=new MyLockImpl();
                    //加锁
                    lock.lock();
                    System.out.println(name+"  locked!");
                    try {
                        //模拟业务处理
                        try {
                            TimeUnit.SECONDS.sleep(10);
                        }catch (InterruptedException e){}
                    }finally {
                        //解锁
                        lock.unlock();
                        System.out.println(name+"  unlock!");
                    }
                }
            }.start();
        }
    }
}
