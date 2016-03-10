package edu.mayo.hadoop.commons.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseConnector;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import org.junit.After;
import org.junit.Before;
import scala.Tuple2;

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

    private String tableName = "SparkHBaseTable";
    private SparkConf sconf;
    private JavaSparkContext sc;
    private Configuration configuration;
    private JavaHBaseContext hbaseContext;
    private HBaseConnector hconnect;
    private HBaseUtil hutil;

    @Before
    public void setup() throws Exception {
        // make a connection on localhost to spark
        //TODO: this needs to use the spark cluster and spark submit if we are on the cluster.
        sconf = new SparkConf().setMaster("local").setAppName("Spark-Hbase Connector");
        sc = new JavaSparkContext(sconf);

        //get a connection to hbase
        configuration = AutoConfigure.getConfiguration();
        hbaseContext = new JavaHBaseContext(sc, configuration);
        hconnect = new HBaseConnector(configuration);
        hutil = new HBaseUtil(hconnect.connect());
    }

    @After
    public void shutdown() throws IOException {
        //sc.stop(); //should this be done here or in the finally clause of each method?
        hconnect.getConnection().close();
    }

    @Test
    public void testConnect() throws Exception {

        ArrayList<Integer> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(i);
        }
        // rdd = sc.parallelize(values).map(x -> x);

    }




    //private JavaHBaseMapGetPutExample() {}

    @Test
    public void getPutTest() {

        try {
            List<byte[]> list = new ArrayList<>();
            list.add(Bytes.toBytes("1"));
            list.add(Bytes.toBytes("2"));
            list.add(Bytes.toBytes("3"));
            list.add(Bytes.toBytes("4"));
            list.add(Bytes.toBytes("5"));

            JavaRDD<byte[]> rdd = sc.parallelize(list);
            //Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);

            hbaseContext.foreachPartition(rdd,
                    new VoidFunction<Tuple2<Iterator<byte[]>, Connection>>() {
                        public void call(Tuple2<Iterator<byte[]>, Connection> t)
                                throws Exception {
                            Table table = t._2().getTable(TableName.valueOf(tableName));
                            BufferedMutator mutator = t._2().getBufferedMutator(TableName.valueOf(tableName));

                            while (t._1().hasNext()) {
                                byte[] b = t._1().next();
                                Result r = table.get(new Get(b));
                                if (r.getExists()) {
                                    mutator.mutate(new Put(b));
                                }
                            }

                            mutator.flush();
                            mutator.close();
                            table.close();
                        }
                    });
        } finally {
            sc.stop();
        }
    }

    public static class GetFunction implements Function<byte[], Get> {
        private static final long serialVersionUID = 1L;
        public Get call(byte[] v) throws Exception {
            return new Get(v);
        }
    }




}
