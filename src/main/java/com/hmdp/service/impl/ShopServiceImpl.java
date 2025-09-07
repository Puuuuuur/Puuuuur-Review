package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
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

    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺详情数据
     */
    public Result queryById(Long id) {
        //缓存穿透问题
        //Shop shop = queryWithPassThrough(id);

        //互斥锁解决缓存击穿问题
        Shop shop = queryWithMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在");//配合穿透问题
        }

        return Result.ok(shop);
    }

    /**
     * 缓存击穿问题：简单互斥锁方法
     * @param id 店铺id
     * @return 店铺信息
     */
    public Shop queryWithMutex(Long id) {
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;

        //从redis查询商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);

        //判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //redis-缓存穿透: 判断命中的是否是空值
        if(shopJson != null){  //是"" 而不是null
            return null;
        }

        //实现缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = trylock(lockKey);

            //判断是否获取成功
            if(!isLock){
                //失败，休眠，并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //成功，根据id查询数据库
            //redis是null，而不是""，根据id查询数据库
            shop = getById(id);
            Thread.sleep(200);

            //redis是null，而不是"", 数据库不存在，直接返回错误
            if(shop == null){
                //redis-缓存穿透：将空值写入redis
                stringRedisTemplate.opsForValue().set(shopKey, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);//2min

                //返回错误信息
                return null;
            }

            //redis不存在，数据库存在，写入redis，设置redis超时时间
            stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//30min

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }

    /**
     * 缓存穿透问题：null空值方法
     * @param id 店铺id
     * @return 店铺信息
     */
    public Shop queryWithPassThrough(Long id) {
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;

        //从redis查询商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);

        //判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //redis-缓存穿透: 判断命中的是否是空值
        if(shopJson != null){  //是"" 而不是null
            return null;
        }

        //redis是null，而不是""，根据id查询数据库
        Shop shop = getById(id);

        //redis是null，而不是"", 数据库不存在，直接返回错误
        if(shop == null){
            //redis-缓存穿透：将空值写入redis
            stringRedisTemplate.opsForValue().set(shopKey, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);//2min

            //返回错误信息
            return null;
        }

        //redis不存在，数据库存在，写入redis，设置redis超时时间
        stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);//30min

        return shop;
    }


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
