package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

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
     * 查询所有商铺类型
     * @return 商铺类型列表
     */
    @Override
    public Result queryTypeList() {
        //从redis中查询商铺缓存
        String shopTypeJSON = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        //判断是否存在
        if (StrUtil.isNotBlank(shopTypeJSON)) {
            //存在，直接返回
            ShopType shopType = JSONUtil.toBean(shopTypeJSON, ShopType.class);
            return Result.ok(shopType);

        }
        //不存在，从数据库查询
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        if (shopTypeList == null) {
            //不存在，返回错误
            return Result.fail("店铺类型不存在");
        }
        //存在，写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypeList));
        return Result.ok(shopTypeList);
    }
}
