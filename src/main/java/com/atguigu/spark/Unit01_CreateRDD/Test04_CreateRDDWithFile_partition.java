package com.atguigu.spark.Unit01_CreateRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author 姜来
 * @ClassName Test04_CreateRDDWithFile_partition.java
 * @createTime 2022年12月20日 10:28:00
 */
public class Test04_CreateRDDWithFile_partition {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Test04_CreateRDDWithFile_partition");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码: 读取文件构建RDD,判断分区
        // 1. 根据文件构建RDD
        JavaRDD<String> javaRDD = sc.textFile("./doc/input/2.txt",3 );
        // 2. 存储
        javaRDD.saveAsTextFile("./doc/output/file");

        // TODO 关闭sc
        sc.stop();
    }
}
