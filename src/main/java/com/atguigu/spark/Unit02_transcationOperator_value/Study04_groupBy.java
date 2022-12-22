package com.atguigu.spark.Unit02_transcationOperator_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study04_groupBy.java
 * @createTime 2022年12月20日 14:41:00
 */
public class Study04_groupBy {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study04_groupBy");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据集合构建RDD,对数据按照奇偶分组
        // 1. 根据集合构建RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        // 2. 分组
        JavaPairRDD<Integer, Iterable<Integer>> groupByRDD = javaRDD.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 % 2;
            }
        });
        // 3. 收集输出
        groupByRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
