package com.izhaowo.cores.test;

import com.izhaowo.cores.engine.XianChiFanUtils;
import com.izhaowo.cores.utils.JavaHBaseUtils;
import com.izhaowo.cores.utils.JavaSparkUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {
    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[4]");
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//
//        JavaRDD<String> all = jsc.textFile("/Users/hiwes/data/izhaowo/worker.txt");
//
//        JavaRDD<String> filter = all.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String l) throws Exception {
//                String planner_id = l.split("\t")[0];
//                if (XianChiFanUtils.workIdIsPlanner(planner_id)) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });
//
//        List<String> top = filter.top(1);
//        for(String s:top){
//            System.out.println(s);
//        }
//
//        System.out.println(filter.count());
//
//
////        Map<String, String> map = new HashMap<>();
////        filter.map(new Function<String, String>() {
////            @Override
////            public String call(String l) throws Exception {
////                String worker_id = l.split("\t")[0];
////                String name = l.split("\t")[1];
////                map.put(worker_id, name);
////                return l;
////            }
////        }).count();

        ResultScanner scanner = JavaHBaseUtils.getScanner("planner_month_rest_remain");

        for (Result s : scanner) {
            String row = Bytes.toString(s.getRow());
            String month = row.split("::")[0];
            String planner_id = row.split("::")[1];

            String todo = Bytes.toString(s.getValue(Bytes.toBytes("info"), Bytes.toBytes("todo")));
            String toget = Bytes.toString(s.getValue(Bytes.toBytes("info"), Bytes.toBytes("toget")));

            String receipt = JavaHBaseUtils.getValue("planner_info", planner_id, "info", "max");
            String limit = receipt;
            if (toget != null) {
                receipt = toget;
            }
            if (todo != null) {
                limit = todo;
            }

            String name = JavaHBaseUtils.getValue("planner_info", planner_id, "info", "name");
            System.out.println(planner_id + "\t" + name + "\t" + month.substring(4,6) + "\t" + month.substring(0,4) + "\t" + receipt + "\t" + limit);
        }

    }
}
