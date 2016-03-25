package hbase;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class VCFParserTestITCase extends TestCase {

    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(VCFParserTestITCase.class);


    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(VCFParserTestITCase.class);

    private static SparkConf sconf;
    private static JavaSparkContext sc;
    private static Configuration configuration;
    private static JavaHBaseContext hbaseContext;
    private static Connection hconnect;
    private static HBaseUtil hutil;
    private static VCFParserConfig config;

    //@Before
    public static void setup() throws Exception {

        config = new VCFParserConfig("src/main/resources/VCFParser.properties");

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
        hutil.createTable(config.getTableName(), config.getColumnFamily());

    }

    //@After
    public static void shutdown() throws IOException {
        // sc.stop(); //should this be done here or in the finally clause of
        // each method?
        hconnect.close();
    }

    @Test
    public void testParser() throws Exception {
        setup();

        //VCFParser parser = new VCFParser(config);
        //parser.parse("/data/VCF/NA_1424005550.gvcf.gz", config.getTableName());

        Result[] results = hutil.first(config.getTableName(), 1000);
        List<String> pretty = hutil.format(results);
        int i = 0;
        for(String line : pretty){
            System.err.println(line);
            i++;
        }

        shutdown();
    }


//    public static void main(String[] args) throws Exception {
//        Configuration configuration = AutoConfigure.getConfiguration();
//        try (Connection hcon = ConnectionFactory.createConnection(configuration)) {
//            HBaseUtil hutil = new HBaseUtil(hcon);
//
//            hutil.createTable(Table, families);
//
//            VCFParserConfig config = new VCFParserConfig("src/main/resources/VCFParser.properties");
//            VCFParser parser = new VCFParser(config);
//            parser.parse("/data/VCF/NA_1424005550.gvcf.gz", Table);
//
//            Result[] results = hutil.first(Table, 1000);
//            List<String> pretty = hutil.format(results);
//            int i = 0;
//            for(String line : pretty){
//                System.out.println(line);
//                i++;
//            }
//        } finally {
//
//        }
//        //parser.shutdown();
//
//
//        //connect
////        HBaseConnector conn = new HBaseConnector();
////        conn.connect();
////        //get schema utilities
////        HBaseSchema schema = new HBaseSchema(conn);
////        //connect to hbase and setup the schema
////        schema.dropAll();
////        schema.setupSchema();
////        //configure the parser
////        VCFParserConfig config = new VCFParserConfig();
////        VCFParser parser = new VCFParser(config, schema);
////
////        //parse the file
////        parser.parse("/data/VCF/NA_1424005550.gvcf.gz");//todo: need to wire in the CLI to enable command line loading
////        conn.close();
//    }

}