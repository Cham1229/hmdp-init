package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;


import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringredistemplate;

    public SimpleRedisLock(StringRedisTemplate stringredistemplate, String name) {
        this.stringredistemplate = stringredistemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";
    //线程标识
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程id
        String threadId =ID_PREFIX + Thread.currentThread().getId() + "";
        // 生成锁的key
        String key = KEY_PREFIX + name;
        Boolean success = stringredistemplate.opsForValue().setIfAbsent(key, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unLock() {
        // 获取线程id，相当于原来锁的值
        String threadId = ID_PREFIX + Thread.currentThread().getId() + "";
        // 获取当前锁的值
        String key = KEY_PREFIX + name;
        String id = stringredistemplate.opsForValue().get(key);
        // 判断锁是否属于当前线程
        if (threadId.equals(id)){
            stringredistemplate.opsForValue().getAndDelete(KEY_PREFIX + name);
        }


    }
}
