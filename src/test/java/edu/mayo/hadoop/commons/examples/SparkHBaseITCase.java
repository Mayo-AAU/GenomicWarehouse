package edu.mayo.hadoop.commons.examples;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by m102417 on 2/15/16.
 *
 * Checks to make sure that Spark and HBase can work together using the
 * HBase-Spark Connector:
 *
 *
 */
public class SparkHBaseITCase {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(SparkHBaseITCase.class);
    @Test
    public void testConnect() throws Exception {

        // make a connection on localhost to spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<Integer> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(i);
        }
        // rdd = sc.parallelize(values).map(x -> x);

    }

}
