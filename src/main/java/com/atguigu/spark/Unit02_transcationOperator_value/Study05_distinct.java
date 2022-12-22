package com.atguigu.spark.Unit02_transcationOperator_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study05_distinct.java
 * @createTime 2022年12月20日 14:49:00
 */
public class Study05_distinct {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study05_distinct");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据集合构建RDD,使用distict对RDD实现去重
        // 1. 根据集合构建RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 1, 2, 4, 5, 5, 4));
        // 2. 对javaRDD实现去重
        JavaRDD<Integer> distinctRDD = javaRDD.distinct();
        // 3. 收集打印
        distinctRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
