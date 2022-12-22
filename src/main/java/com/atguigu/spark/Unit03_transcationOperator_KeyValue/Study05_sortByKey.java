package com.atguigu.spark.Unit03_transcationOperator_KeyValue;

import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study05_sortByKey.java
 * @createTime 2022年12月20日 16:03:00
 */
public class Study05_sortByKey {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study05_sortByKey");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 构建一个kvRDD，对其按照key进行排序
        // 1. 构建kvRDD
        JavaPairRDD<Integer, String> kvRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, "haha"), new Tuple2<>(3, "hehe"), new Tuple2<>(4, "heihei"), new Tuple2<>(2, "xixi")
        ));
        // 2. 对RDD按照key进行排序-- 降序
        JavaPairRDD<Integer, String> sortByKeyRDD = kvRDD.sortByKey(false);
        // 3. 收集打印
        sortByKeyRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
