package com.atguigu.spark.Unit04_actionOperator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * @author 姜来
 * @ClassName Study02_actionOperator.java
 * @createTime 2022年12月21日 09:19:00
 */
public class Study02_actionOperator {
    public static void main(String[] args) {
        // TODO 创建SparkConf
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Study02_actionOperator");


        // TODO 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // TODO 业务逻辑代码
        // 1. 构建kv类型RDD
        JavaRDD<String> javaRDD = sc.parallelize(Arrays.asList("a", "b", "c","b","c"));
        JavaPairRDD<String, Integer> kvRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        // 2. 总计相同key的个数
        Map<String, Long> resultMap = kvRDD.countByKey();
        // 3. 遍历
        Set<Map.Entry<String, Long>> entries = resultMap.entrySet();
        for (Map.Entry<String, Long> entry : entries) {
            System.out.println(entry.getKey()+", "+entry.getValue());
        }
        // 3. kvRDD存储
//        kvRDD.saveAsTextFile("./doc/output/kText");
//        kvRDD.saveAsObjectFile("./doc/output/kObject");
        // 4. 输出kvRDD
        kvRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t2) throws Exception {
                System.out.println(t2._1+", "+t2._2);
            }
        });
        // TODO 关闭sc
        sc.stop();
    }
}
