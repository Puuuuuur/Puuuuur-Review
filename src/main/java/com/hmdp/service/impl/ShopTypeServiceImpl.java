package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询所有商铺类型(添加缓存)
     * @return 商铺类型列表
     */
    public Result queryTypeList() {
        //查询缓存
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        String shopTypeList = stringRedisTemplate.opsForValue().get(key);

        //判断缓存是否命中
         if(StrUtil.isNotBlank(shopTypeList)){
             //缓存命中
             List<ShopType> shopTypes = JSONUtil.toList(shopTypeList, ShopType.class);
             //根据sort字段排序
             Collections.sort(shopTypes, (o1, o2) -> o1.getSort() - o2.getSort());
             return Result.ok(shopTypes);
         }

         //缓存未命中，查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
         //判断数据库中是否有数据
        if(CollectionUtils.isEmpty(shopTypes)){
            //数据库中无数据，返回错误
            return Result.fail("商铺类型不存在");
        }
        //存入缓存，返回数据
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypes));

        return Result.ok(shopTypes);
    }
}
