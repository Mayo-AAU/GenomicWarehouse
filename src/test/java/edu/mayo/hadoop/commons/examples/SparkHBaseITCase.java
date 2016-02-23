package edu.mayo.hadoop.commons.examples;

import it.nerdammer.spark.hbase.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Test;

import java.util.ArrayList;
import java.util.stream.IntStream;

import static java.util.stream.IntStream.*;

/**
 * Created by m102417 on 2/15/16.
 *
 * Checks to make sure that Spark and HBase can work together using the HBase-Spark Connector:
 *
 *
 */
public class SparkHBaseITCase {


    @Test
    public void testConnect(){

        //make a connection on localhost to spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList values = new ArrayList();
        for(int i=0;i<100;i++){values.add(i);}
        //RDD rdd = sc.parallelize(values).map(i -> (i.toString(), i+1, "Hello"))



    }

}
