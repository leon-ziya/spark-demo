package com.atguigu.spark.Unit02_transcationOperator_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author 姜来
 * @ClassName Study02_flatMap.java
 * @createTime 2022年12月20日 14:23:00
 */
public class Study02_flatMap {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study02_flatMap");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据集合构建RDD, 将旧RDD的每个元素炸裂
        // 1. 根据集合构建RDD
        JavaRDD<List<Integer>> javaRDD = sc.parallelize(Arrays.asList(
                Arrays.asList(1, 2), Arrays.asList(3, 4), Arrays.asList(5, 6), Arrays.asList(7)
        ));
        System.out.println("输出javaRDD==========");
        javaRDD.collect().forEach(System.out::println);
        // 2. 用flatMap算子对RDD进行炸裂
        JavaRDD<Integer> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> integers) throws Exception {
                return integers.iterator();
            }
        });
        // 3. 收集打印
        System.out.println("输出flatMapRDD==========");
        flatMapRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
