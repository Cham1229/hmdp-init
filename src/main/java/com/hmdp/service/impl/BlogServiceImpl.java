package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private FollowServiceImpl followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->  {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);

    }

    @Override
    public Result queryBlogById(Long id) {
        // 查询博客
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        // 查询用户
        queryBlogUser(blog);
        //查询点赞数量
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //获取用户
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return;
        }
        Long userId = user.getId();
        //判断当前用户是否已经点赞
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + blog.getId(), userId.toString());
        blog.setIsLike(score!= null);
    }

        /**
     * 用户对博客进行点赞或取消点赞操作
     * @param id 博客ID
     * @return 操作结果
     */
    @Override
    public Result likeBlog(Long id) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否已经点赞
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + id, userId.toString());
        if (score!= null) {
            //如果已经点赞，取消点赞
            //数据库中点赞数-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //把用户从缓存中删除
            if (isSuccess){
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKED_KEY + id, userId.toString());
            }
        } else {
            //否则，点赞
            //数据库中点赞数+1
            //把用户保存到缓存中
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess){
                stringRedisTemplate.opsForZSet().add(BLOG_LIKED_KEY + id, userId.toString(),System.currentTimeMillis());
            }

        }

        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 查询top5
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);
        // 解析出id
        if (top5 == null || top5.isEmpty()) {
            return Result.fail("暂无点赞用户");
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //根据id查询用户
        String idStr = StrUtil.join(",", ids);
        List<User> users = userService.query().in("id", ids).last("order by field( id," + idStr + ")").list();
        // 修复：将User转换为UserDTO并收集到列表中
        List<UserDTO> userDTOS = users.stream().map(user -> {
            UserDTO userDTO = new UserDTO();
            BeanUtil.copyProperties(user, userDTO);
            return userDTO;
        }).collect(Collectors.toList());

        return Result.ok(userDTOS);  // 返回转换后的UserDTO列表
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("保存博客失败");
        }
         //查询笔记作者的粉丝
        List<Follow> follows = followService.query().eq("follow_id", blog.getUserId()).list();
        //推送笔记
        for (Follow follow : follows) {
            Long userId = follow.getUserId();
            // 推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        return Result.ok(blog.getId());
    }

    /**
     * 查询关注用户的博客（滚动分页）
     * <p>
     * 该方法用于实现关注用户的博客信息流功能，基于 Redis 的 ZSet 实现时间轴排序。
     * 通过当前用户 ID 构造收件箱键，从 Redis 中获取按时间戳倒序排列的博客 ID，
     * 并结合数据库查询返回对应的博客列表及滚动分页相关信息。
     *
     * @param max    当前时间戳上限，用于范围查询 Redis ZSet 中的分数（时间戳）
     * @param offset 偏移量，用于分页控制，表示从符合条件的数据中跳过的数量
     * @return Result 包含博客列表、最小时间戳和新的偏移量，用于前端滚动加载
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();

        // 查询收件箱：根据时间范围从 Redis ZSet 中获取指定数量的博客 ID 及其时间戳
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        // 解析 Redis 返回结果，提取博客 ID 和时间戳，并计算最小时间和偏移量
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int ost = 1;
        for(ZSetOperations.TypedTuple<String> typedTuple : typedTuples){
            // 获取博客 ID
            String id = typedTuple.getValue();
            ids.add(Long.valueOf(id));
            // 获取时间戳（分数）
            long time = typedTuple.getScore().longValue();
            if (time == minTime){
                ost++;
            }else {
                ost = 1;
                minTime = time;
            }
        }

        // 根据 ID 列表查询博客，并保持与 Redis 中相同的顺序
        /**
         * 不使用 listByIds() 而使用 query().in().last() 的主要原因是需要保持结果的顺序
         * 从 Redis ZSet 中获取的 ID 列表是按时间戳排序的，需要在查询数据库时保持这个顺序
         * listByIds() 的限制：
         * listByIds() 方法不保证返回结果的顺序与输入 ID 列表的顺序一致
         * 数据库默认按主键顺序返回记录，无法控制排序
         * 这会导致前端显示的博客顺序与时间顺序不一致
         */
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            // 查询博客相关的用户信息
            queryBlogUser(blog);
            // 查询当前用户是否点赞该博客
            isBlogLiked(blog);
        }

        // 封装滚动分页结果并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(ost);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }



    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
