package com.hmdp.service.impl;

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
        String shopKey = RedisConstants.CACHE_SHOP_KEY + id;

        //从redis查询商铺信息
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);

        //判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        //redis不存在，根据id查询数据库
        Shop shop = getById(id);

        //redis不存在，数据库不存在，直接返回错误
        if(shop == null){
            return Result.fail("店铺不存在");
        }

        //redis不存在，数据库存在，写入redis，设置redis超时时间
        stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return Result.ok(shop);
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
}
