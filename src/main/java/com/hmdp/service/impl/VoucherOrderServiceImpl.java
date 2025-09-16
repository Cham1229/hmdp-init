package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.jetbrains.annotations.NotNull;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

import static cn.hutool.core.util.DesensitizedUtil.userId;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringredistemplate;
    

    @Override
    public Result seckillVoucher(Long voucherId) {
        //查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
            //秒杀尚未开始
            return Result.fail("秒杀尚未开始");
        }

        //判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
            //秒杀已经结束
            return Result.fail("秒杀已经结束");
        }

        //判断库存是否充足
        if (voucher.getStock() < 1){
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringredistemplate, "order:" + userId);
        boolean isLock = simpleRedisLock.tryLock(1000L);
        if (!isLock){
            //获取锁失败
            return Result.fail("请勿重复下单");
        }

        try {
            //获取代理对象
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }finally {
            simpleRedisLock.unLock();
        }

//        synchronized (userId.toString().intern()){
//            //获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }

    }
    @NotNull
    @Transactional
    public synchronized Result createVoucherOrder(Long voucherId) {
        //一人一单
        Long userId = UserHolder.getUser().getId();

        //查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        //判断是否存在
        if (count > 0){
            //存在
            return Result.fail("不能重复下单");
        }

        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")//set stock = stock - 1
                .eq("voucher_id", voucherId).gt("stock", 0)//where stock > 0 and id = ?
                .update();
        if (!success){
            //扣减库存失败
            return Result.fail("库存不足");
        }

        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id
        voucherOrder.setUserId(userId);
        //优惠券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        //返回订单id
        return Result.ok(voucherOrder.getId());


    }
}
