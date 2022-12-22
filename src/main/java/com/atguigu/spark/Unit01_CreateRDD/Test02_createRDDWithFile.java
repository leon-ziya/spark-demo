package com.atguigu.spark.Unit01_CreateRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author 姜来
 * @ClassName Test02_createRDDWithFile.java
 * @createTime 2022年12月20日 10:08:00
 */
public class Test02_createRDDWithFile {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Test02_createRDDWithFile");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 根据文件构建RDD
        // 1. 根据文件构建RDD
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/1.txt");
        // 2. 收集打印
        javaRDD.collect().forEach(System.out::println);

        // TODO 关闭sc
        sc.stop();
    }
}
