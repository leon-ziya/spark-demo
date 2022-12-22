package com.atguigu.spark.Unit06_serializer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoSerializer;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study01_serializer.java
 * @createTime 2022年12月21日 10:20:00
 */
public class Study01_serializer {
    public static void main(String[] args) throws ClassNotFoundException {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_serializer")
                .set("spark.serializer", KryoSerializer.class.getName())
                .registerKryoClasses(new Class[]{Class.forName(User.class.getName())});


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 构建RDD，里面的元素是对象
        // 1. 根据文件构建RDD
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/user.txt");
        // 2. 将javaRDD中每个元素包装为对象
        JavaRDD<User> userRDD = javaRDD.map(new Function<String, User>() {
            @Override
            public User call(String v1) throws Exception {
                String[] splits = v1.split(",");
                return new User(splits[0], Integer.valueOf(splits[1]), splits[2]);
            }
        });
        // 3. 修改每个用户的学校
        JavaRDD<User> user01RDD = userRDD.map(new Function<User, User>() {
            @Override
            public User call(User user) throws Exception {
                if (!user.getSchool().equals("尚硅谷")) {
                    user.setSchool("尚硅谷");
                }
                return user;
            }
        });

        // 4. 收集打印
        user01RDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
