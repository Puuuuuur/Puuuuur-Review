package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

public class SimpleRedisLock implements ILock{

    private static final String key_prefix = "lock:"; //锁名称的前缀
    private String name; //业务的名称-锁名称

    private StringRedisTemplate stringRedisTemplate; //通过构造函数传入

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate){
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 尝试获取锁
     * @param timeoutSec 锁超时时间
     * @return
     */
    public boolean tryLock(int timeoutSec) {
        //获取线程ID
        long threadId = Thread.currentThread().getId();

        //获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(key_prefix + name, threadId + "", timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success); //拆箱注意空指针风险
    }

    /**
     * 释放锁
     */
    public void unLock() {
        //释放锁
        stringRedisTemplate.delete(key_prefix + name);
    }
}
