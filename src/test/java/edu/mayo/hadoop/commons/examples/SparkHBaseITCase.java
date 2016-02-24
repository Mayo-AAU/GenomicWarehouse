package edu.mayo.hadoop.commons.examples;

import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;
import it.nerdammer.spark.hbase.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
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


    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(SparkHBaseITCase.class);
    // properties file
    public static final String HBASE_PROPS = "/hbase.properties";
    private static Properties props;

    static {
        try (InputStream stream = HBaseITCase.class.getResourceAsStream(HBASE_PROPS)) {
            props = MiniClusterUtil.loadPropertiesStream(stream);
        } catch (IOException e) {
            LOG.error("Unable to load property file: {}", HBASE_PROPS);
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        MiniClusterUtil.startHBASE(props);
    }

    @Test
    public void testConnect()throws Exception {

        //make a connection on localhost to spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList values = new ArrayList();
        for(int i=0;i<100;i++){values.add(i);}
        // rdd = sc.parallelize(values).map(x -> x);






    }

}
