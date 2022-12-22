package com.atguigu.spark.Unit04_actionOperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author 姜来
 * @ClassName Study01_actionOperator.java
 * @createTime 2022年12月21日 09:07:00
 */
public class Study01_actionOperator {
    public static void main(String[] args) throws InterruptedException {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_actionOperator");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 学习使用action算子
        // 1. 从集合构建RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        // 2. 收集打印
        javaRDD.collect().forEach(System.out::println);

        // 3. 统计RDD中的元素个数
        long count = javaRDD.count();
        System.out.println("javaRDD中元素的个数："+count);
        // 4. 获取javaRDD中第一个元素
        Integer first = javaRDD.first();
        System.out.println("javaRDD中的第一个元素是："+first);
        // 5. 获取javaRDD中前3个元素
        List<Integer> ThreeList = javaRDD.take(3);
        System.out.println("javaRDD中前三个元素："+ThreeList.toString());

        Thread.sleep(100000);
        // TODO 关闭sc
        sc.stop();
    }
}
