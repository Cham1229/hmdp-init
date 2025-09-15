package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

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
    @Override
    public Result queryById(Long id) throws InterruptedException {
        // 解决缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        // 逻辑过期解决缓存击穿
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        return Result.ok(shop);
    }

    public static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


        /**
     * 通过逻辑过期方式查询商铺信息（带缓存重建机制）
     * <p>
     * 该方法首先从Redis缓存中查询指定ID的商铺数据。如果缓存未命中或已过期，
     * 则尝试获取分布式锁并异步重建缓存，同时立即返回当前过期数据以保证响应性能。
     * </p>
     *
     * @param id 商铺ID，用于标识要查询的商铺
     * @return 商铺信息对象；若缓存中不存在则返回null
     */
    public Shop queryWithLogicalExpire(Long id){
        // 1.从Redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        if (StrUtil.isBlank(shopJson)){
            // 3.不存在，直接返回
            return null;
        }
        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断Redis中的数据是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            // 5.2.未过期，直接返回数据
            return shop;
        }

        // 5.1.过期，需要缓存重建

        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取成功
        if (isLock){
            // 6.3.成功，执行缓存重建
            if (expireTime.isAfter(LocalDateTime.now())){//再次检测redis缓存是否过期
                // 5.2.未过期，直接返回数据
                return shop;
            }
            // 2. 提交异步任务到线程池
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                // 重建缓存
                this.saveShop2Redis(id, 20L);
                // 释放锁
                unLock(lockKey);

            });

        }


        // 6.4.返回过期商铺信息
        return shop;
    }


        /**
     * 使用互斥锁查询商铺信息
     *
     * @param id 商铺ID
     * @return 商铺信息，如果不存在则返回null
     * @throws InterruptedException 线程中断异常
     */
    public Shop queryWithMutex(Long id) throws InterruptedException {
        // 1.从Redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            // 3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null){              //如果是空值
            return null;
        }

        // 获取互斥锁，防止缓存击穿
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 4.判断是否获取成功
        if (!isLock){
            //获取锁失败，等待后重试
            Thread.sleep(50);
            return queryWithMutex( id);
        }

        // 3.不存在，查询数据库
        Shop shop = getById(id);
        // 5.数据库不存在，返回错误
        if (shop == null){
            //将空值写入Redis，防止缓存穿透
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在，写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 释放互斥锁
        unLock(lockKey);

        return shop;
    }


    /**
     * 根据ID查询商铺信息，使用Redis缓存穿透解决方案
     *
     * @param id 商铺ID
     * @return 商铺信息，如果不存在则返回null
     */
    public Shop queryWithPassThrough(Long id){
        // 1.从Redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            // 3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson != null){              //如果是空值
            return null;
        }

        // 3.不存在，查询数据库
        Shop shop = getById(id);
        // 5.数据库不存在，返回错误
        if (shop == null){
            //将空值写入Redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 6.存在，写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 逻辑过期
     * @param id
     * @param expireSeconds
     */
    private void saveShop2Redis(Long id, Long expireSeconds){
        // 查询
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }
}
