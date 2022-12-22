package com.atguigu.spark.Unit02_transcationOperator_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study06_sortBy.java
 * @createTime 2022年12月20日 14:54:00
 */
public class Study06_sortBy {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study06_sortBy");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据集合构建RDD,使用sortBy算子对其进行排序
        // 1. 根据集合构建RDD
        JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(3,4,5,2,1,7,6));
        // 2. 对javaRDD进行排序
        JavaRDD<Integer> sortByRDD = javaRDD.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, true, 2);
        // 3. 收集打印
        sortByRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
