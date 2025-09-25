import com.hmdp.HmDianPingApplication;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest(classes = HmDianPingApplication.class)
public class ImportCoordinateTest {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IShopService shopService;

    @Test
    void loadShopData(){
        // 1.查询店铺数据
        List<Shop> list = shopService.list();
        // 2.把店铺分组，按照type_id分组，type_id一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

        // 3.分批写入
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1.获取类型id
            Long typeId = entry.getKey();
            // 3.2.获取同类型的店铺数据
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            // 3.3.写入redis
            String key = SHOP_GEO_KEY + typeId;
            for (Shop shop : value){
                //stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

        /**
     * 测试Redis的HyperLogLog功能
     *
     * 该测试方法演示了如何使用Redis的HyperLogLog数据结构来统计大量数据的基数（去重计数）。
     * 通过模拟一百万个用户数据的添加过程，验证HyperLogLog的准确性和性能表现。
     *
     * 测试流程：
     * 1. 创建包含1000个元素的字符串数组作为批量操作的缓冲区
     * 2. 循环生成一百万个用户数据，格式为"user_" + 序号
     * 3. 每1000个数据为一批次添加到Redis的HyperLogLog结构中
     * 4. 最后统计HyperLogLog中的基数大小并输出结果
     */
    @Test
    void testHyperLogLog(){
        String[] values = new String[1000];
        // 批量添加数据到HyperLogLog，每1000个为一批次
        for (int i = 0; i < 1000000; i++){
            values[i%1000] = "user_" + i;
            if (i % 1000 == 999){
                stringRedisTemplate.opsForHyperLogLog().add("hl1", values);
            }
        }
        // 统计数量
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl1");
        System.out.println("count = " + count);

    }

}
