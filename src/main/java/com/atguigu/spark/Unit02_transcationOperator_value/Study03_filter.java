package com.atguigu.spark.Unit02_transcationOperator_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author 姜来
 * @ClassName Study03_filter.java
 * @createTime 2022年12月20日 14:33:00
 */
public class Study03_filter {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study03_filter");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据文件构建RDD,保留其中偶数
        // 1. 根据1.txt构建RDD
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/1.txt");
        // 2. 转换数据类型，由String---> Integer
        JavaRDD<Integer> mapRDD = javaRDD.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return Integer.valueOf(v1);
            }
        });
        // 3. 过滤
        JavaRDD<Integer> filterRDD = mapRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });

        // 4. 收集打印
        filterRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
