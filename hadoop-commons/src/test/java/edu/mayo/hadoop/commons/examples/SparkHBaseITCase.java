package edu.mayo.hadoop.commons.examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
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
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SparkHBaseITCase implements Serializable {

    private static final long serialVersionUID = 1L;

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(SparkHBaseITCase.class);

    private static final String tableName = "SparkHBaseTable";
    private static final String[] columnFamily = {"cf1", "cf2"};
    private static SparkConf sconf;
    private static JavaSparkContext sc;
    private static Configuration configuration;
//    private static JavaHBaseContext hbaseContext;
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
//        hbaseContext = new JavaHBaseContext(sc, configuration);
        hconnect = ConnectionFactory.createConnection(configuration);
        hutil = new HBaseUtil(hconnect);
        hutil.dropTable(tableName);
        hutil.createTable(tableName, new String[]{"cf1"});

    }

    @AfterClass
    public static void shutdown() throws Exception {
        // sc.stop(); //should this be done here or in the finally clause of
        // each method?
    	sc.close();
    	sc.stop();
        hconnect.close();
        AutoConfigure.stop();
    }

    @Test
    public void test1Connect() throws Exception {
        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
    }

    // private JavaHBaseMapGetPutExample() {}

    @Test
    public void test2BulkPut() throws Exception {

        try {
        	List<String> list = readGVCF("src/test/resources/testData/example.gvcf");
        	
            JavaRDD<String> rdd = sc.parallelize(list);

            // Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);

            hbaseContext.bulkPut(rdd, TableName.valueOf(tableName), new PutFunction());
        } finally {
            // not sure I should stop this thing here!
//            sc.stop();
        }

        // go look at what the heck is in the table
        System.err.println("*************************************************");
        Result[] r = hutil.first(tableName, 10);
        List<String> pretty = hutil.format(r);
        for (String next : pretty) {
            System.err.println(next);
        }
        
        System.err.println ("Test bulkPut succeeded");
        
    }

    public static final byte[] cf1Bytes = Bytes.toBytes("cf1");
    public static final byte[] chrBytes = Bytes.toBytes("chr");
    public static final byte[] posBytes = Bytes.toBytes("pos");
    public static final byte[] idBytes = Bytes.toBytes("id");
    public static final byte[] refBytes = Bytes.toBytes("ref");
    public static final byte[] altBytes = Bytes.toBytes("alt");
    public static final byte[] qualBytes = Bytes.toBytes("qual");
    public static final byte[] filterBytes = Bytes.toBytes("filter");
    public static final byte[] infoBytes = Bytes.toBytes("info");
    public static final byte[] sampleBytes = Bytes.toBytes("sample1");
    
    public static class PutFunction implements Function<String, Put> {
        private static final long serialVersionUID = 1L;
        @Override
        public Put call(String v) throws Exception {
            String[] cells = v.split("\t");
            Put put = new Put(Bytes.toBytes(cells[0] + ":" + cells[1] + ":" + cells[3] + "/" + cells [4]));
            put.addColumn(cf1Bytes, chrBytes, Bytes.toBytes(cells[0]));
            put.addColumn(cf1Bytes, posBytes, Bytes.toBytes(cells[1]));
            put.addColumn(cf1Bytes, idBytes, Bytes.toBytes(cells[2]));
            put.addColumn(cf1Bytes, refBytes, Bytes.toBytes(cells[3]));
            put.addColumn(cf1Bytes, altBytes, Bytes.toBytes(cells[4]));
            put.addColumn(cf1Bytes, qualBytes, Bytes.toBytes(cells[5]));
            put.addColumn(cf1Bytes, filterBytes, Bytes.toBytes(cells[6]));
            put.addColumn(cf1Bytes, infoBytes, Bytes.toBytes(cells[7]));
//            put.addColumn(cf1Bytes, formatBytes, Bytes.toBytes(cells[8]));
            put.addColumn(cf1Bytes, sampleBytes, (new VcfFormatFields(cells[8], cells[9])).serialize());
            return put;	
        }
    }
    

    /**
     * test bulk get for a specified list of keys and process them line by line as they are retrieved (like write to a file)
     * @throws Exception
     */
    @Test
    public void test3BulkGet() throws Exception {
        try {
            List<byte[]> list = new ArrayList<>();
            list.add(Bytes.toBytes("chr13:25074425:G/A"));
            list.add(Bytes.toBytes("chr16:50343418:G/."));
            list.add(Bytes.toBytes("chr16:50343460:C/."));
            list.add(Bytes.toBytes("chr1:1386020:G/A")); // this row does not exist

            JavaRDD<byte[]> rdd = sc.parallelize(list);
            // Configuration conf = HBaseConfiguration.create();

            JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
            hbaseContext.foreachPartition(rdd, new VoidFunction<Tuple2<Iterator<byte[]>, Connection>>() {
                @Override
                public void call(Tuple2<Iterator<byte[]>, Connection> t) throws Exception {
                    Table table = t._2().getTable(TableName.valueOf(tableName));
                    File file = new File ("src/test/resources/testData/output.vcf");
                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    System.out.println("writing to file: " + file.getAbsolutePath());
        			BufferedWriter bw = new BufferedWriter(fw);
        			int rowCount = 0;
                    while (t._1().hasNext()) {
                        byte[] b = t._1().next();
                        Result r = table.get(new Get(b));
                        byte[] rb = r.getValue(cf1Bytes, chrBytes);
                        if (rb!=null) {
                        	StringBuffer buf = new StringBuffer();
                        	String cVal = Bytes.toString(rb);
                        	buf.append(cVal)
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, posBytes)))
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, idBytes)))
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, refBytes)))
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, altBytes)))
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, qualBytes)))
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, filterBytes)))
                        	   .append("\t").append(Bytes.toString(r.getValue(cf1Bytes, infoBytes)))
                        	   .append("\t").append(VcfFormatFields.deserialize(r.getValue(cf1Bytes, sampleBytes)).toString())
                        	   .append("\n");
                        	bw.write(buf.toString());
                        	rowCount++;
                        }
                    }
                    bw.close();
                    table.close();
                    System.err.println("total vcf rows written: " + rowCount);
                    if (rowCount != 3) throw new Exception ("Failed to extract the exact number of vcf lines from HBase");
                }
            });
        } finally {
//            sc.stop();
        }
        
        System.err.println("Testing bulkGet inline processing succeeded");
    }

    
    private List<String> readGVCF (String inFileVCF) throws Exception {
    	List<String> retList = sc.textFile(inFileVCF).filter(s -> !s.startsWith("#")).collect();
    	return retList;
    }
    
    /**
     * test bulk get into RDD for a specified set of keys
     * @throws Exception
     */
    @Test
    public void test4BulkGet () throws Exception {
        List<byte[]> list = new ArrayList<>();
        list.add(Bytes.toBytes("chr1:875063:G/."));
        list.add(Bytes.toBytes("chr10:101419628:G/."));
        list.add(Bytes.toBytes("chr10:101419712:C/."));
        list.add(Bytes.toBytes("chr1:1386020:G/A"));
System.err.println("running test4BulkGet");
        JavaRDD<byte[]> rdd = sc.parallelize(list);
        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
        JavaRDD<String> rdd2 = hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetFunction(), new ResultFunction());
        List<String> resultList = rdd2.collect();
        for (int i=0; i< resultList.size(); i++) {
        	if (resultList.get(i)!=null) {
        		System.err.println(resultList.get(i));
        	}
        }
System.err.println("end test4BulkGet retrieval into RDD");
    }
    
    public static class GetFunction implements Function<byte[], Get> {
    	private static final long serialVersionUID = 1L;
    	public Get call(byte[] v) throws Exception {
    		return new Get(v);
    	}
    }
    
    public static class ResultFunction implements Function<Result,String> {
    	private static final long serialVersionUID = 1L;
    	public String call(Result result) throws Exception {
    		String ret = VcfFormatFields.toVCF(result);
    		return ret;
    	}
    	
    }
    
    /**
     * 
     * @throws IOException
     */
    @Test
    public void test5GetWithFilter () throws Exception {
        System.err.println("test5GetWithFilter chrX started");
    	
    	 Result[] r = hutil.firstWitFilter(tableName, "chrX", 10);
    	 
         List<String> pretty = hutil.format(r);
         for (String next : pretty) {
             System.err.println(next);
         }
         
    	 if (r.length != 8) throw new Exception ("Wrong number of results returned for chrX, expecting 8, returned " + r.length);
         
         System.err.println("test5GetWithFilter chrX succeeded");
    }
    

}
