package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 将任意Java对象序列化为JSON字符串后存储到Redis缓存中，并设置过期时间
     */
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 使用逻辑过期的方式将任意Java对象序列化为JSON字符串后存储到Redis缓存中
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 解决缓存穿透问题：将""写入缓存的方法
     * 当数据库中也查询不到数据时，将""写入Redis缓存，防止频繁查询数据库
     * @param keyPrefix 缓存键前缀
     * @param id 数据标识符（如店铺ID）
     * @param type 返回值类型
     * @param dbFallback 数据库查询函数，当Redis中查询不到时调用此函数查询数据库
     * @param time 缓存过期时间
     * @param unit 时间单位
     * @param <R> 返回值泛型
     * @param <ID> ID泛型
     * @return 查询结果（可能为空）
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        //从redis查询商铺信息
        String json = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            //存在，直接返回
            return JSONUtil.toBean(json, type);
        }

        //redis-缓存穿透: 判断命中的是否是空值
        if(json != null){  //是"" 而不是null
            return null;
        }

        //redis是null，而不是""，根据id查询数据库
        R r = dbFallback.apply(id);

        //redis是null，而不是"", 数据库不存在，直接返回错误
        if(r == null){
            //redis-缓存穿透：将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);//2min

            //返回错误信息
            return null;
        }

        //redis不存在，数据库存在，写入redis，设置redis超时时间
        this.set(key, r, time, unit);

        return r;
    }

    //自定义线程池
    private static final ExecutorService CHCHE_REBUILD_EXECTURE = Executors.newFixedThreadPool(10);

    /**
     * 缓存击穿问题：逻辑过期方法
     * @param id 店铺id
     * @return 店铺信息
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        //从redis查询商铺信息
        String json = stringRedisTemplate.opsForValue().get(key);

        //判断是否存在
        if(StrUtil.isBlank(json)){
            //不存在，直接返回null
            return null;
        }

        //命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期，直接返回店铺信息
            return r;
        }

        //已过期，需要缓存重建
        // 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = trylock(lockKey);

        //判断是否获取锁成功
        if(isLock){
            //成功，开启独立线程，实现缓存重建
            CHCHE_REBUILD_EXECTURE.submit(() -> {
                try {
                    //重建缓存:先查数据库
                    R r1 = dbFallback.apply(id);

                    //重建缓存:写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);

                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //失败，直接返回过期的商铺信息
        return r;
    }

    /**
     * 尝试获取锁
     * @param key 锁的key
     * @return 是否获取锁
     */
    private boolean trylock(String key){
        //尝试上锁，当锁的key不存在则创建
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key 锁的key
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

}
