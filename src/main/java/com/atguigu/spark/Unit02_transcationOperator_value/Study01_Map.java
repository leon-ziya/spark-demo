package com.atguigu.spark.Unit02_transcationOperator_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author 姜来
 * @ClassName Study01_Map.java
 * @createTime 2022年12月20日 14:13:00
 */
public class Study01_Map {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study01_Map");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 读取1.txt构建RDD, 对RDD中每个元素拼接“||”
        // 1. 根据磁盘构建RDD
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/1.txt");
        // 2. 使用map算子,对其中每个元素尾部拼接“||”
        JavaRDD<String> mapRDD = javaRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + "||";
            }
        });
        // 3. 收集打印
        mapRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
