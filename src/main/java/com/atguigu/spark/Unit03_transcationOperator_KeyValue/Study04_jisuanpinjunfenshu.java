package com.atguigu.spark.Unit03_transcationOperator_KeyValue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Study04_jisuanpinjunfenshu.java
 * @createTime 2022年12月20日 15:49:00
 */
public class Study04_jisuanpinjunfenshu {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study04_jisuanpinjunfenshu");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 构建kv类型RDD，统计平均分数
        // 1. 构建kvRDD
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("张三", 62), new Tuple2<>("张三", 72), new Tuple2<>("张三", 82), new Tuple2<>("张三", 92),
                new Tuple2<>("李四", 62), new Tuple2<>("李四", 62), new Tuple2<>("李四", 62), new Tuple2<>("李四", 62)
        ));

        // 2. 使用mapValus聚合 ("张三“， 62) --> ("张三”，（62，1））
        JavaPairRDD<String, Tuple2<Integer, Integer>> mapValues01RDD = pairRDD.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                return new Tuple2<>(v1, 1);
            }
        });

        // 2. 使用reduceBykey聚合 ("张三“， （总和，总个数）)
        JavaPairRDD<String, Tuple2<Integer, Integer>> reduceByKeyRDD = mapValues01RDD.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
            }
        });
        // 3. 使用mapValues对上述RDD计算平均分数
        JavaPairRDD<String, Double> pingjunRDD = reduceByKeyRDD.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> v1) throws Exception {
                return Double.valueOf(v1._1) / v1._2;
            }
        });
        // 4. 收集输出
        pingjunRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
