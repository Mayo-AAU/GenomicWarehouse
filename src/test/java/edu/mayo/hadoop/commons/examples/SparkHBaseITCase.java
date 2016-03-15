package edu.mayo.hadoop.commons.examples;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import scala.Tuple2;

/**
 * Created by m102417 on 2/15/16.
 *
 * Checks to make sure that Spark and HBase can work together using the
 * HBase-Spark Connector:
 *
 *
 */
public class SparkHBaseITCase implements Serializable {

    private static final long serialVersionUID = 1L;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(SparkHBaseITCase.class);

    private static final String tableName = "SparkHBaseTable";
    private static final String[] columnFamily = {"cf1", "cf2"};
    private static SparkConf sconf;
    private static JavaSparkContext sc;
    private static Configuration configuration;
    private static JavaHBaseContext hbaseContext;
    private static Connection hconnect;
    private static HBaseUtil hutil;

    @BeforeClass
    public static void setup() throws Exception {
        // make a connection on localhost to spark
        // TODO: this needs to use the spark cluster and spark submit if we are
        // on the cluster.
        sconf = new SparkConf().setMaster("local").setAppName("Spark-Hbase Connector");
        sconf.set("spark.driver.host", "127.0.0.1");

        sc = new JavaSparkContext(sconf);

        // get a connection to hbase
        configuration = AutoConfigure.getConfiguration();
        hbaseContext = new JavaHBaseContext(sc, configuration);
        hconnect = ConnectionFactory.createConnection(configuration);
        hutil = new HBaseUtil(hconnect);
        hutil.createTable(tableName, new String[]{"cf1"});

    }

    @AfterClass
    public static void shutdown() throws IOException {
        // sc.stop(); //should this be done here or in the finally clause of
        // each method?
        hconnect.close();
    }

    @Test
    public void testConnect() throws Exception {

        ArrayList<Integer> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            values.add(i);
        }
        // rdd = sc.parallelize(values).map(x -> x);

    }

    // private JavaHBaseMapGetPutExample() {}

    @Test
    public void testBulkPut() throws IOException {


        try {
            List<String> list = new ArrayList<>();
            list.add("1," + columnFamily[0] + ",a,1");
            list.add("2," + columnFamily[0] + ",a,2");
            list.add("3," + columnFamily[0] + ",a,3");
            list.add("4," + columnFamily[0] + ",a,4");
            list.add("5," + columnFamily[0] + ",a,5");

            JavaRDD<String> rdd = sc.parallelize(list);

            //Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);

            hbaseContext.bulkPut(rdd,
                    TableName.valueOf(tableName),
                    new PutFunction());
        } finally {
            //not sure I should stop this thing here!
            sc.stop();
        }

        //go look at what the heck is in the table
        System.err.println("*************************************************");
        Result[] r = hutil.first(tableName, 1000);
        List<String> pretty = hutil.format(r);
        for(String next : pretty){
            System.err.println(next);
        }
    }



    public static class PutFunction implements Function<String, Put> {
        private static final long serialVersionUID = 1L;
        public Put call(String v) throws Exception {
            String[] cells = v.split(",");
            Put put = new Put(Bytes.toBytes(cells[0]));
            put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
                    Bytes.toBytes(cells[3]));
            return put;
        }
    }

    //@Test
    public void getPutTest() throws Exception {
        try {
            List<byte[]> list = new ArrayList<>();
            list.add(Bytes.toBytes("1"));
            list.add(Bytes.toBytes("2"));
            list.add(Bytes.toBytes("3"));
            list.add(Bytes.toBytes("4"));
            list.add(Bytes.toBytes("5"));

            JavaRDD<byte[]> rdd = sc.parallelize(list);
            // Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);

            hbaseContext.foreachPartition(rdd, new VoidFunction<Tuple2<Iterator<byte[]>, Connection>>() {
                @Override
                public void call(Tuple2<Iterator<byte[]>, Connection> t) throws Exception {
                    Table table = t._2().getTable(TableName.valueOf(tableName));
                    BufferedMutator mutator = t._2().getBufferedMutator(TableName.valueOf(tableName));

                    while (t._1().hasNext()) {
                        byte[] b = t._1().next();
                        Result r = table.get(new Get(b));
                        // getExists() may return null...
                        Boolean exists = r.getExists();
                        if (exists != null && exists) {
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

        //go look at what the heck is in the table
        System.err.println("*************************************************");
        Result[] r = hutil.first(tableName, 1000);
        List<String> pretty = hutil.format(r);
        for(String next : pretty){
            System.err.println(next);
        }
    }

    public static class GetFunction implements Function<byte[], Get> {
        private static final long serialVersionUID = 1L;
        @Override
        public Get call(byte[] v) throws Exception {
            return new Get(v);
        }
    }

}
