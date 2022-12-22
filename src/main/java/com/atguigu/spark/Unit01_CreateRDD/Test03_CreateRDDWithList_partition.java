package com.atguigu.spark.Unit01_CreateRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author 姜来
 * @ClassName Test03_CreateRDDWithList_partition.java
 * @createTime 2022年12月20日 10:14:00
 */
public class Test03_CreateRDDWithList_partition {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Test03_CreateRDDWithList_partition");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 测试根据 集合构建RDD时，RDD的分区
        JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("1", "2", "3", "4", "5"),3);
        // 1. 存储到文件中
        javaRDD.saveAsTextFile("./doc/output/list");

        // TODO 关闭sc
        sc.stop();
    }
}
