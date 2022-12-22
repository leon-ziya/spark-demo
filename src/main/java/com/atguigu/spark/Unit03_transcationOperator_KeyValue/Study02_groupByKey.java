package com.atguigu.spark.Unit03_transcationOperator_KeyValue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study02_groupByKey.java
 * @createTime 2022年12月20日 15:31:00
 */
public class Study02_groupByKey {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study02_groupByKey");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 创建一个kv类型RDD, 按照key进行分组，统计每组内value的综和
        // 1. 构建KVRDD
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1), new Tuple2<>("a", 1), new Tuple2<>("a", 1), new Tuple2<>("a", 1),
                new Tuple2<>("b", 1), new Tuple2<>("b", 1), new Tuple2<>("b", 1), new Tuple2<>("b", 1)
        ));
        // 2. 按照key进行分组
        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = pairRDD.groupByKey();
        // 3. 对value值进行累加求和
        JavaPairRDD<String, Integer> mapValuesRDD = groupByKeyRDD.mapValues(new Function<Iterable<Integer>, Integer>() {
            @Override
            public Integer call(Iterable<Integer> v1) throws Exception {
                Integer sum = 0;
                for (Integer integer : v1) {
                    sum += integer;
                }
                return sum;
            }
        });
        // 4. 收集打印
        mapValuesRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
