package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringredistemplate;

    @Resource
    private RedissonClient redissonClient;

    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    private final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private RedisTemplate<Object, Object> redisTemplate;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    //获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //获取用户
            Long userId = voucherOrder.getUserId();
            //创建锁对象
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //获取锁
            boolean isLock = lock.tryLock();
            if (!isLock){
                //获取锁失败
                log.error("不允许重复下单");
                return;
            }
            try {
                 proxy.createVoucherOrder(voucherOrder);
            }finally {
                lock.unlock();
            }
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        Long result = stringredistemplate.execute
                (SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), UserHolder.getUser().getId().toString());
        if(result != 0){
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        //创建订单，把购买信息放到阻塞队列
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        //创建阻塞队列
        orderTasks.add(voucherOrder);
        //获取代理对象
         proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) throws InterruptedException {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            //秒杀尚未开始
//            return Result.fail("秒杀尚未开始");
//        }
//
//        //判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            //秒杀已经结束
//            return Result.fail("秒杀已经结束");
//        }
//
//        //判断库存是否充足
//        if (voucher.getStock() < 1){
//            return Result.fail("库存不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        //SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringredistemplate, "order:" + userId);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁
//        boolean isLock = lock.tryLock();
//        if (!isLock){
//            //获取锁失败
//            return Result.fail("请勿重复下单");
//        }
//        try {
//            //获取代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }
//
/// /        synchronized (userId.toString().intern()){
/// /            //获取代理对象
/// /            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
/// /            return proxy.createVoucherOrder(voucherId);
/// /        }
//
//    }



    @Transactional
    public synchronized void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();

        //查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //判断是否存在
        if (count > 0){
            //存在
            log.error("不能重复下单");
            return;
        }

        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")//set stock = stock - 1
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)//where stock > 0 and id = ?
                .update();
        if (!success){
            //扣减库存失败
            log.error("库存不足");
            return ;
        }

        save(voucherOrder);

    }
}
