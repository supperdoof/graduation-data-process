package com.spark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.stringtemplate.v4.ST;
import scala.Int;
import scala.Tuple2;
import scala.Tuple5;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by hadoop on 10/2/17.
 */
public class Hive {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Hive").setMaster("spark://master:7077").set("spark.executor.memory", "4g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("ERROR");
        HiveContext hiveContext = new HiveContext(javaSparkContext);


        /*
        JavaRDD<String> dataSet1 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/1.txt");
        JavaRDD<String> dataSet2 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/2.txt");
        JavaRDD<String> dataSet3 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/3.txt");
        JavaRDD<String> dataSet4 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/4.txt");
        JavaRDD<String> dataSet5 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/5.txt");
        JavaRDD<String> dataSet6 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/6.txt");
        JavaRDD<String> dataSet7 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/7.txt");
        JavaRDD<String> dataSet8 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/8.txt");
        JavaRDD<String> dataSet9 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/9.txt");
        JavaRDD<String> dataSet10 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/10.txt");
        JavaRDD<String> dataSet11 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/11.txt");
        JavaRDD<String> dataSet12 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/12.txt");
        JavaRDD<String> dataSet13 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/13.txt");
        JavaRDD<String> dataSet14 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/14.txt");
        JavaRDD<String> dataSet15 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170930/15.txt");
        //JavaRDD<String> dataSet16 = javaSparkContext.textFile("file:///home/hadoop/桌面/20170927/16.txt");

        JavaRDD<String> dataSet = dataSet1.union(dataSet2).union(dataSet3).union(dataSet4).union(dataSet5).union(dataSet6).union(dataSet7).union(dataSet8).union(dataSet9).union(dataSet10).union(dataSet11).union(dataSet12).union(dataSet13).union(dataSet14).union(dataSet15);
        */

        System.out.println();
        System.out.println();
        System.out.println();


        /*
        JavaPairRDD<String, Tuple5<Double, String, String, Long, Long>> pairRDD = dataSet.flatMapToPair(new PairFlatMapFunction<String, String, Tuple5<Double, String, String, Long, Long>>() {
            @Override
            public Iterator<Tuple2<String, Tuple5<Double, String, String, Long, Long>>> call(String s) throws Exception {
                List<Tuple2<String, Tuple5<Double, String, String, Long, Long>>> list = new ArrayList<>();
                try {
                    Gson gson = new Gson();
                    Information information = new Information();
                    Type type = new TypeToken<Information>() {}.getType();
                    information = gson.fromJson(s,type);
                    for (Data data : information.getData())
                    {
                        list.add(new Tuple2<>(information.getAccountid(),new Tuple5<>(data.getSerialNumber(),data.getPackageName(),data.getPageName(),Long.parseLong(data.getStartTime()),Long.parseLong(data.getEndTime()))));
                    }

                }catch (Exception e)
                {
                    e.printStackTrace();
                }

                return list.iterator();


            }
        });

        System.out.println("--------------------"+pairRDD.count()+"---------------------------");

        */




        /*
        JavaRDD<String> deviceid = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("deviceid");
            }
        });

        System.out.println("---------------The Deviceid Result----------------");
        for (String s : deviceid.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> accountid = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("accountid");

            }
        });

        System.out.println("---------------The Accountid Result----------------");
        for (String s : accountid.collect())
        {
            System.out.println(s);
        }

        JavaRDD<Long> date = dataSet.map(new Function<String, Long>() {
            @Override
            public Long call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return Long.parseLong(map.get("date"));

            }
        });

        System.out.println("---------------The Date Result----------------");
        for (Long s : date.collect())
        {
            System.out.println(s);
        }


        JavaRDD<String> os_info_mac = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                String os_info = new Gson().toJson(gson.fromJson(s, Map.class).get("os_info"));
                Map<String,String> map = gson.fromJson(os_info, Map.class);
                return map.get("mac");

            }
        });

        System.out.println("---------------The Os_info_mac Result----------------");
        for (String s : os_info_mac.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> os_info_os = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                String os_info = new Gson().toJson(gson.fromJson(s, Map.class).get("os_info"));
                Map<String,String> map = gson.fromJson(os_info, Map.class);
                return map.get("os");

            }
        });

        System.out.println("---------------The Os_info_os Result----------------");
        for (String s : os_info_os.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> os_info_device_id = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                String os_info = new Gson().toJson(gson.fromJson(s, Map.class).get("os_info"));
                Map<String,String> map = gson.fromJson(os_info, Map.class);
                return map.get("device_id");

            }
        });

        System.out.println("---------------The Os_info_device_id Result----------------");
        for (String s : os_info_device_id.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> os_info_os_version = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                String os_info = new Gson().toJson(gson.fromJson(s, Map.class).get("os_info"));
                Map<String,String> map = gson.fromJson(os_info, Map.class);
                return map.get("os_version");

            }
        });

        System.out.println("---------------The Os_info_os_version Result----------------");
        for (String s : os_info_os_version.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> versioncode = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("versioncode");

            }
        });

        System.out.println("---------------The Versioncode Result----------------");
        for (String s : versioncode.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> channel = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("channel");

            }
        });

        System.out.println("---------------The Channel Result----------------");
        for (String s : channel.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> resolution = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("resolution");

            }
        });

        System.out.println("---------------The Resolution Result----------------");
        for (String s : resolution.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> access = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("access");

            }
        });

        System.out.println("---------------The Access Result----------------");
        for (String s : access.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> ip = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("ip");

            }
        });

        System.out.println("---------------The Ip Result----------------");
        for (String s : ip.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> cpu = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("cpu");

            }
        });

        System.out.println("---------------The Cpu Result----------------");
        for (String s : cpu.collect())
        {
            System.out.println(s);
        }

        JavaRDD<Integer> operators = dataSet.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return Integer.parseInt(map.get("operators"));

            }
        });

        System.out.println("---------------The Operators Result----------------");
        for (Integer s : operators.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> network_type = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("network_type");

            }
        });

        System.out.println("---------------The Network_type Result----------------");
        for (String s : network_type.collect())
        {
            System.out.println(s);
        }

        JavaRDD<String> subtype = dataSet.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,String> map = gson.fromJson(s, Map.class);
                return map.get("subtype");

            }
        });

        System.out.println("---------------The Subtype Result----------------");
        for (String s : subtype.collect())
        {
            System.out.println(s);
        }

        JavaRDD<Double> version = dataSet.map(new Function<String, Double>() {
            @Override
            public Double call(String s) throws Exception {
                Gson gson = new  Gson();
                Map<String,Double> map = gson.fromJson(s, Map.class);
                return map.get("version");
            }
        });

        System.out.println("---------------The Version Result----------------");
        for (Double s : version.collect())
        {
            System.out.println(s);
        }
        */



        /*
        hiveContext.sql("use default");


        JavaRDD<Row> rowJavaRDD = pairRDD.map(new Function<Tuple2<String, Tuple5<Double, String, String, Long, Long>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple5<Double, String, String, Long, Long>> stringTuple5Tuple2) throws Exception {
                data data = new  data();
                data.setAccountId(stringTuple5Tuple2._1());
                data.setSerialNumber(stringTuple5Tuple2._2()._1());
                data.setPackageName(stringTuple5Tuple2._2()._2());
                data.setPageName(stringTuple5Tuple2._2()._3());
                data.setStartTime(stringTuple5Tuple2._2()._4());
                data.setEndTime(stringTuple5Tuple2._2()._5());
                return data.toRow();
            }
        });

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("accountId",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("serialNumber",DataTypes.DoubleType,true));
        fields.add(DataTypes.createStructField("packageName",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("pageName",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("startTime",DataTypes.LongType,true));
        fields.add(DataTypes.createStructField("endTime",DataTypes.LongType,true));

        StructType structType = DataTypes.createStructType(fields);

        DataFrameWriter dataFrameWriter = hiveContext.createDataFrame(rowJavaRDD,structType).repartition(10).write();
        dataFrameWriter.mode(SaveMode.Append).saveAsTable("information_20170930");
        System.out.println("Load Data Finished!!!!!");

        */




        /*
        Dataset<Row> sqlResult_20170912 = hiveContext.sql("select accountid,pagename from information_20170912 where substring(pagename,1,11)=\"com.tencent\" ");
        Dataset<Row> sqlResult_20170913 = hiveContext.sql("select accountid,pagename from information_20170913 where substring(pagename,1,11)=\"com.tencent\"");
        Dataset<Row> sqlResult_20170925 = hiveContext.sql("select accountid,pagename from information_20170925 where substring(pagename,1,11)=\"com.tencent\"");
        Dataset<Row> sqlResult_20170926 = hiveContext.sql("select accountid,pagename from information_20170926 where substring(pagename,1,11)=\"com.tencent\"");
        Dataset<Row> sqlResult_20170927 = hiveContext.sql("select accountid,pagename from information_20170927 where substring(pagename,1,11)=\"com.tencent\"");
        Dataset<Row> sqlResult_20170928 = hiveContext.sql("select accountid,pagename from information_20170928 where substring(pagename,1,11)=\"com.tencent\"");
        Dataset<Row> sqlResult_20170929 = hiveContext.sql("select accountid,pagename from information_20170929 where substring(pagename,1,11)=\"com.tencent\"");
        Dataset<Row> sqlResult_20170930 = hiveContext.sql("select accountid,pagename from information_20170930 where substring(pagename,1,11)=\"com.tencent\"");

        JavaRDD<Row> sqlResultRdd_20170912 = sqlResult_20170912.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170913 = sqlResult_20170913.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170925 = sqlResult_20170925.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170926 = sqlResult_20170926.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170927 = sqlResult_20170927.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170928 = sqlResult_20170928.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170929 = sqlResult_20170929.toJavaRDD();
        JavaRDD<Row> sqlResultRdd_20170930 = sqlResult_20170930.toJavaRDD();

        Dataset<Row> sqlResultTotal_20170912 = hiveContext.sql("select accountid,pagename from information_20170912 ");
        Dataset<Row> sqlResultTotal_20170913 = hiveContext.sql("select accountid,pagename from information_20170913 ");
        Dataset<Row> sqlResultTotal_20170925 = hiveContext.sql("select accountid,pagename from information_20170925 ");
        Dataset<Row> sqlResultTotal_20170926 = hiveContext.sql("select accountid,pagename from information_20170926 ");
        Dataset<Row> sqlResultTotal_20170927 = hiveContext.sql("select accountid,pagename from information_20170927 ");
        Dataset<Row> sqlResultTotal_20170928 = hiveContext.sql("select accountid,pagename from information_20170928 ");
        Dataset<Row> sqlResultTotal_20170929 = hiveContext.sql("select accountid,pagename from information_20170929 ");
        Dataset<Row> sqlResultTotal_20170930 = hiveContext.sql("select accountid,pagename from information_20170930 ");

        JavaRDD<Row> sqlResultTotalRdd_20170912 = sqlResultTotal_20170912.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170913 = sqlResultTotal_20170913.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170925 = sqlResultTotal_20170925.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170926 = sqlResultTotal_20170926.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170927 = sqlResultTotal_20170927.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170928 = sqlResultTotal_20170928.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170929 = sqlResultTotal_20170929.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170930 = sqlResultTotal_20170930.toJavaRDD();

        JavaRDD<Row> sqlResultRdd = sqlResultRdd_20170912.union(sqlResultRdd_20170913).union(sqlResultRdd_20170925).union(sqlResultRdd_20170926).union(sqlResultRdd_20170927).union(sqlResultRdd_20170928).union(sqlResultRdd_20170929).union(sqlResultRdd_20170930);
        JavaRDD<Row> sqlResultTotalRdd = sqlResultTotalRdd_20170912.union(sqlResultTotalRdd_20170913).union(sqlResultTotalRdd_20170925).union(sqlResultTotalRdd_20170926).union(sqlResultTotalRdd_20170927).union(sqlResultTotalRdd_20170928).union(sqlResultTotalRdd_20170929).union(sqlResultTotalRdd_20170930);

        System.out.println("--------------"+sqlResultTotalRdd.count()+"-------------");

        JavaPairRDD<String,Integer> pageVisitorNumber = sqlResultRdd.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(1),1);
            }
        });

        JavaPairRDD<String,Integer> pageVisitorNumberCount = pageVisitorNumber.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaPairRDD<String,Integer> accountNumber = sqlResultTotalRdd.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        });

        JavaPairRDD<String,Integer> accountNumberCount = accountNumber.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaPairRDD<String,String> accountPageName = sqlResultRdd.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),row.getString(1));
            }
        });

        //----------------------------------------------------------------------------------------


        JavaPairRDD<Integer,String> pageVisitorNumberCountSort = pageVisitorNumberCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2(),stringIntegerTuple2._1());
            }
        }).sortByKey(false);


        System.out.println("-------------------------PageVisitorTotalNumberCountSort Result----------------------");
        for (Tuple2<Integer,String> s : pageVisitorNumberCountSort.collect())
        {
            System.out.println(s);
            break;
        }

        //pageVisitorNumberCountSort.saveAsTextFile("file:///home/hadoop/Desktop/Sort");



        System.out.println("---------------------------"+accountNumberCount.count()+"-------------------------");

        final long accountTotalNumber = accountNumberCount.count();

        JavaPairRDD<Integer,String> pageVisitorNumberCountSortFilter = pageVisitorNumberCountSort.filter(new Function<Tuple2<Integer, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                if (integerStringTuple2._2().substring(0,11).equals("com.tencent"))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        });

        pageVisitorNumberCountSortFilter.saveAsTextFile("file:///home/hadoop/桌面/TencentSort");

        JavaPairRDD<Double,String> pageVisitorNumberCountSortFilterAverage = pageVisitorNumberCountSortFilter.mapToPair(new PairFunction<Tuple2<Integer, String>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._1()*1.0/accountTotalNumber,integerStringTuple2._2());
            }
        });

        pageVisitorNumberCountSortFilterAverage.saveAsTextFile("file:///home/hadoop/桌面/TencentSortAverage");

        List<String> pageVisitorTotalNumberCountSortFilterAverageTakeThree = pageVisitorNumberCountSortFilterAverage.values().take(4);
        final String first = pageVisitorTotalNumberCountSortFilterAverageTakeThree.get(1);
        final String second = pageVisitorTotalNumberCountSortFilterAverageTakeThree.get(2);
        final String third = pageVisitorTotalNumberCountSortFilterAverageTakeThree.get(3);

        System.out.println(first);
        System.out.println(second);
        System.out.println(third);



        JavaRDD<String> accountPageNameUnionFilterFirst = accountPageName.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if (stringStringTuple2._2().equals(first))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._1();
            }
        });

        JavaRDD<String> accountPageNameUnionFilterSecond = accountPageName.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if (stringStringTuple2._2().equals(second))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._1();
            }
        });

        JavaRDD<String> accountPageNameUnionFilterThird = accountPageName.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if (stringStringTuple2._2().equals(third))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._1();
            }
        });

        System.out.println("---------------------"+accountPageNameUnionFilterFirst.count()+"----------------------------");
        System.out.println("---------------------"+accountPageNameUnionFilterSecond.count()+"----------------------------");
        System.out.println("---------------------"+accountPageNameUnionFilterThird.count()+"----------------------------");






        List<String> firstList = accountPageNameUnionFilterFirst.collect();
        List<String> secondList = accountPageNameUnionFilterSecond.collect();
        List<String> thirdList = accountPageNameUnionFilterThird.collect();

        final List<String> coincidenceList = new ArrayList<>();

        for (String s1 : firstList)
        {
            for (String s2 : secondList)
            {
                if (s1.equals(s2))
                {
                    for (String s3 : thirdList)
                    {
                        if (s2.equals(s3))
                        {
                            coincidenceList.add(s3);
                            break;
                        }
                    }
                    break;
                }
            }
        }

        System.out.println("---------------------"+coincidenceList.size()+"----------------------------");

        final String transfer = "com.tencent.mm.plugin.remittance.ui.RemittanceDetailUI";

        JavaPairRDD<String,String> accountPageNameUnionFilterTransfer = accountPageName.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                if (stringStringTuple2._2().equals(transfer))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                boolean flag = false;
                for (String s : coincidenceList)
                {
                    if (stringStringTuple2._1().equals(s))
                    {
                        flag = true;
                        break;
                    }
                }

                if (flag)
                {
                    return true;
                }
                else
                {
                    return false;
                }

            }
        });

        System.out.println("----------------"+accountPageNameUnionFilterTransfer.count()+"-------------------------");

        */


        /*
        Dataset<Row> sqlResultTotal_20170912 = hiveContext.sql("select accountid,pagename from information_20170912 ");
        Dataset<Row> sqlResultTotal_20170913 = hiveContext.sql("select accountid,pagename from information_20170913 ");
        Dataset<Row> sqlResultTotal_20170925 = hiveContext.sql("select accountid,pagename from information_20170925 ");
        Dataset<Row> sqlResultTotal_20170926 = hiveContext.sql("select accountid,pagename from information_20170926 ");
        Dataset<Row> sqlResultTotal_20170927 = hiveContext.sql("select accountid,pagename from information_20170927 ");
        Dataset<Row> sqlResultTotal_20170928 = hiveContext.sql("select accountid,pagename from information_20170928 ");
        Dataset<Row> sqlResultTotal_20170929 = hiveContext.sql("select accountid,pagename from information_20170929 ");
        Dataset<Row> sqlResultTotal_20170930 = hiveContext.sql("select accountid,pagename from information_20170930 ");

        JavaRDD<Row> sqlResultTotalRdd_20170912 = sqlResultTotal_20170912.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170913 = sqlResultTotal_20170913.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170925 = sqlResultTotal_20170925.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170926 = sqlResultTotal_20170926.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170927 = sqlResultTotal_20170927.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170928 = sqlResultTotal_20170928.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170929 = sqlResultTotal_20170929.toJavaRDD();
        JavaRDD<Row> sqlResultTotalRdd_20170930 = sqlResultTotal_20170930.toJavaRDD();

        JavaPairRDD<String,Integer> pairRDD_20170912 = sqlResultTotalRdd_20170912.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170913 = sqlResultTotalRdd_20170913.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170925 = sqlResultTotalRdd_20170925.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170926 = sqlResultTotalRdd_20170926.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170927 = sqlResultTotalRdd_20170927.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170928 = sqlResultTotalRdd_20170928.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170929 = sqlResultTotalRdd_20170929.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<String,Integer> pairRDD_20170930 = sqlResultTotalRdd_20170930.mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });



        System.out.println("20170912: " + pairRDD_20170912.count());
        System.out.println("20170913: " + pairRDD_20170913.count());
        System.out.println("20170925: " + pairRDD_20170925.count());
        System.out.println("20170926: " + pairRDD_20170926.count());
        System.out.println("20170927: " + pairRDD_20170927.count());
        System.out.println("20170928: " + pairRDD_20170928.count());
        System.out.println("20170929: " + pairRDD_20170929.count());
        System.out.println("20170930: " + pairRDD_20170930.count());

        */



        /*

        Dataset<Row> sqlResult = hiveContext.sql("select accountid from information_20170912 ");

        JavaRDD<Row> sqlResultRdd = sqlResult.javaRDD();

        JavaPairRDD<String,Long> sqlResultPairRdd = sqlResultRdd.mapToPair(new PairFunction<Row, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),1L);
            }
        });

        System.out.println("--------------"+sqlResultPairRdd.count()+"---------------------");

        */







    }
}
