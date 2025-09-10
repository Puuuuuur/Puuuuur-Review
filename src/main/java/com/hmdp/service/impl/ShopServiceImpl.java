package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.RedisData;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺详情数据
     */
    public Result queryById(Long id) {
        //缓存穿透问题
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿问题
        // Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿问题
        //Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if(shop == null){
            return Result.fail("店铺不存在");//配合穿透问题
        }

        return Result.ok(shop);
    }


    //自定义线程池
    private static final ExecutorService CHCHE_REBUILD_EXECTURE = Executors.newFixedThreadPool(10);

    /**
     * 缓存击穿问题：逻辑过期方法
     * @param id 店铺id
     * @return 店铺信息
     */
    // public Shop queryWithLogicalExpire(Long id) {
    //     String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
    //
    //     //从redis查询商铺信息
    //     String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
    //
    //     //判断是否存在
    //     if(StrUtil.isBlank(shopJson)){
    //         //不存在，直接返回null
    //         return null;
    //     }
    //
    //     //命中，需要先把json反序列化为对象
    //     RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
    //     JSONObject data = (JSONObject) redisData.getData();
    //     Shop shop = JSONUtil.toBean(data, Shop.class);
    //     LocalDateTime expireTime = redisData.getExpireTime();
    //
    //     //判断是否过期
    //     if(expireTime.isAfter(LocalDateTime.now())){
    //         //未过期，直接返回店铺信息
    //         return shop;
    //     }
    //
    //     //已过期，需要缓存重建
    //     // 获取互斥锁
    //     String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
    //     boolean isLock = trylock(lockKey);
    //
    //     //判断是否获取锁成功
    //     if(isLock){
    //         //成功，开启独立线程，实现缓存重建
    //         CHCHE_REBUILD_EXECTURE.submit(() -> {
    //             try {
    //                 //重建缓存
    //                 this.saveShop2Redis(id, 20L);
    //             }catch (Exception e){
    //                 throw new RuntimeException(e);
    //             }finally {
    //                 //释放锁
    //                 unLock(lockKey);
    //             }
    //         });
    //     }
    //     //失败，直接返回过期的商铺信息
    //     return shop;
    // }


    /**
     * 缓存击穿问题：简单互斥锁方法
     * @param id 店铺id
     * @return 店铺信息
     */
    // public Shop queryWithMutex(Long id) {
    //     String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
    //
    //     //从redis查询商铺信息
    //     String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
    //
    //     //判断是否存在
    //     if(StrUtil.isNotBlank(shopJson)){
    //         //存在，直接返回
    //         Shop shop = JSONUtil.toBean(shopJson, Shop.class);
    //         return shop;
    //     }
    //
    //     //redis-缓存穿透: 判断命中的是否是空值
    //     if(shopJson != null){  //是"" 而不是null
    //         return null;
    //     }
    //
    //     //实现缓存重建
    //     //获取互斥锁
    //     String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
    //     Shop shop = null;
    //     try {
    //         boolean isLock = trylock(lockKey);
    //
    //         //判断是否获取成功
    //         if(!isLock){
    //             //失败，休眠，并重试
    //             Thread.sleep(50);
    //             return queryWithMutex(id);
    //         }
    //
    //         //成功，根据id查询数据库
    //         //redis是null，而不是""，根据id查询数据库
    //         shop = getById(id);
    //         Thread.sleep(200);
    //
    //         //redis是null，而不是"", 数据库不存在，直接返回错误
    //         if(shop == null){
    //             //redis-缓存穿透：将空值写入redis
    //             stringRedisTemplate.opsForValue().set(shopKey, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);//2min
    //
    //             //返回错误信息
    //             return null;
    //         }
    //
    //         //redis不存在，数据库存在，写入redis，设置redis超时时间
    //         stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//30min
    //
    //     } catch (Exception e) {
    //         throw new RuntimeException(e);
    //     } finally {
    //         //释放互斥锁
    //         unLock(lockKey);
    //     }
    //     return shop;
    // }

    /**
     * 缓存穿透问题：null空值方法
     * @param id 店铺id
     * @return 店铺信息
     */
    // public Shop queryWithPassThrough(Long id) {
    //     String shopKey = RedisConstants.CACHE_SHOP_KEY + id;
    //
    //     //从redis查询商铺信息
    //     String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
    //
    //     //判断是否存在
    //     if(StrUtil.isNotBlank(shopJson)){
    //         //存在，直接返回
    //         Shop shop = JSONUtil.toBean(shopJson, Shop.class);
    //         return shop;
    //     }
    //
    //     //redis-缓存穿透: 判断命中的是否是空值
    //     if(shopJson != null){  //是"" 而不是null
    //         return null;
    //     }
    //
    //     //redis是null，而不是""，根据id查询数据库
    //     Shop shop = getById(id);
    //
    //     //redis是null，而不是"", 数据库不存在，直接返回错误
    //     if(shop == null){
    //         //redis-缓存穿透：将空值写入redis
    //         stringRedisTemplate.opsForValue().set(shopKey, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);//2min
    //
    //         //返回错误信息
    //         return null;
    //     }
    //
    //     //redis不存在，数据库存在，写入redis，设置redis超时时间
    //     stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//30min
    //
    //     return shop;
    // }


    /**
     * 更新商铺信息
     * @param shop 商铺数据
     * @return 无
     */
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }

        //更新数据库
        updateById(shop);

        //删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());

        return Result.ok();
    }
    //
    // /**
    //  * 尝试获取锁
    //  * @param key 锁的key
    //  * @return 是否获取锁
    //  */
    // private boolean trylock(String key){
    //     //尝试上锁，当锁的key不存在则创建
    //     Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
    //     return BooleanUtil.isTrue(flag);
    // }
    //
    // /**
    //  * 释放锁
    //  * @param key 锁的key
    //  */
    // private void unLock(String key){
    //     stringRedisTemplate.delete(key);
    // }

    /**
     * 提前保存店铺数据到redis-预热
     * @param id 店铺id
     * @param expireSeconds 逻辑过期时间
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
