package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringredistemplate;

    public SimpleRedisLock(StringRedisTemplate stringredistemplate, String name) {
        this.stringredistemplate = stringredistemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程id
        String value = Thread.currentThread().getId() + "";
        // 生成锁的key
        String key = KEY_PREFIX + name;
        Boolean success = stringredistemplate.opsForValue().setIfAbsent(key, value, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unLock() {
        stringredistemplate.opsForValue().getAndDelete(KEY_PREFIX + name);
    }
}
