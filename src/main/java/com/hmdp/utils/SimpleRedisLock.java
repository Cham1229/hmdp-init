package com.hmdp.utils;


import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;


import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private final String name;
    private final StringRedisTemplate stringredistemplate;

    public SimpleRedisLock(StringRedisTemplate stringredistemplate, String name) {
        this.stringredistemplate = stringredistemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";
    //线程标识
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

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
        //调用lua脚本
        stringredistemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(KEY_PREFIX + name), ID_PREFIX + Thread.currentThread().getId());
    }
//    @Override
//    public void unLock() {
//        // 获取线程id，相当于原来锁的值
//        String threadId = ID_PREFIX + Thread.currentThread().getId() + "";
//        // 获取当前锁的值
//        String key = KEY_PREFIX + name;
//        String id = stringredistemplate.opsForValue().get(key);
//        // 判断锁是否属于当前线程
//        if (threadId.equals(id)) {
//            stringredistemplate.opsForValue().getAndDelete(KEY_PREFIX + name);
//        }
//    }
}
