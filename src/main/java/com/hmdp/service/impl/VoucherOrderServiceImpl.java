package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
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
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private RedisTemplate<Object, Object> redisTemplate;
    private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
                    //获取队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> seckillOrder = stringredistemplate.opsForStream().read(
                            Consumer.from("g1","c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //判断消息是否获取成功
                    if (seckillOrder == null || seckillOrder.isEmpty()){
                        //如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    //解析数据
                    MapRecord<String, Object, Object> record = seckillOrder.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //获取成功，下单
                    handleVoucherOrder(voucherOrder);
                    //ACK确认  SACK stream.orders g1 id
                    stringredistemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
//        @Override
//        public void run() {
//            while (true){
//                try {
//                    //获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    //创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
        }

        /**
         * 处理Redis Stream中的pending list消息
         *
         * 该方法用于处理Redis Stream中未确认的消息列表，通过消费者组机制确保消息的可靠处理。
         * 方法会持续读取pending list中的消息，直到没有更多消息需要处理为止。
         * 对于每条读取到的消息，会解析成订单对象并进行处理，处理成功后发送ACK确认。
         *
         * 注意：该方法包含无限循环，通过内部条件控制循环结束
         */
        private void handlePendingList() {
            // 持续处理pending list中的消息
            while (true){
                try {
                    // 获取队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> seckillOrder = stringredistemplate.opsForStream().read(
                            Consumer.from("g1","c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 判断消息是否获取成功
                    if (seckillOrder == null || seckillOrder.isEmpty()){
                        // 如果获取失败，说明pending list没有异常消息，结束循环
                        break;
                    }

                    // 解析数据并处理订单
                    MapRecord<String, Object, Object> record = seckillOrder.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 获取成功，下单
                    handleVoucherOrder(voucherOrder);

                    // ACK确认  SACK stream.orders g1 id
                    stringredistemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                }

                // 休眠2秒后继续处理下一批消息
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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

    /**
     * 秒杀优惠券
     * @param voucherId 优惠券ID
     * @return 秒杀结果，包含订单ID或错误信息
     * @throws InterruptedException 当线程中断时抛出
     */
    @Override
    public Result seckillVoucher(Long voucherId) throws InterruptedException {
        // 获取当前用户ID
        long userId = UserHolder.getUser().getId();
        // 生成订单ID
        long orderId = redisIdWorker.nextId("order");

        // 执行秒杀脚本，检查库存和是否重复下单
        Long result = stringredistemplate.execute
                (SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId, String.valueOf(orderId));
        if(result != 0){
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        //创建订单，把购买信息放到阻塞队列

        // 构建订单对象
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
