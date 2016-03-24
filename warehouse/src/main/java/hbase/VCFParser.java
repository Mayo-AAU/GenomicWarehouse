package hbase;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.genomics.model.Variant;
import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseConnector;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;
import edu.mayo.pipes.UNIX.CatPipe;
import hbase.util.HBaseSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by m102417 on 1/26/16.
 *
 *  This is a simple class that parses a single sample VCF file and pushes it to HBase
 *
 */
public class VCFParser implements Serializable {

    private VCFParserConfig config;
    private HBaseSchema schema;

    /**
     * constructing a VCFParser requires a parser config object to ensure that
     * all of the options are accounted for.
     * @param config
     */
    public VCFParser(VCFParserConfig config) throws Exception {
        this.config = config;
        startup();
    }

    public void startup() throws Exception {
        //startup the hbase client
        Configuration config = AutoConfigure.getConfiguration();
        HBaseConnector conn = new HBaseConnector(config);
        //todo:this is causing problems!
        //conn.connect();
        //schema = new HBaseSchema(conn);
    }

    public void shutdown() throws IOException {
        schema.close();
    }

    /**
     *
     * @param sc
     * @param VCFFilePath
     * @param maxHeader    - the maximum number of lines we want to process (makes it go much faster as long as people don't give us million line headers
     */
    private List<String> getHeader(JavaSparkContext sc, String VCFFilePath, int maxHeader){
        List<String> header = sc.textFile(VCFFilePath).filter(s -> s.startsWith("#")).take(maxHeader);
        //print the header
//        for(String line : header){
//            System.err.println(line);
//        }
        return header;
    }

    /**
     * Assumes a VCF file is on HDFS.  this Parser uses Spark to parallize the rows,
     *
     *
     * @param inputVCF - the input path for the vcf file
     */
    public void parse(String inputVCF, String tableName) throws ParseException, IOException {

        JavaSparkContext sc = new JavaSparkContext(this.config.getSparkConfiguration());
        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, config.getHBaseConfig());
        List<String> header = getHeader(sc,inputVCF,config.getMaxHeader());
        try {
            JavaRDD<String> vcflines = sc.textFile(inputVCF).filter(s -> !s.startsWith("#"));

            hbaseContext.bulkPut(vcflines, TableName.valueOf(tableName), new PutVCFRowFunction());
        }finally {
            sc.stop();
        }

        System.out.println("Finished");

    }

    public static final String Table = "VCFTable";
    public static final String[] families = {"samples"};
    public static final String LINE = "line";

    public static class PutVCFRowFunction implements Function<String, Put> {
        private static final long serialVersionUID = 1L;
        public Put call(String s) throws Exception {
            String[] cells = s.split("\t");
            Variant v = new Variant(s);
            Put put = new Put(Bytes.toBytes(v.hash()));
            put.addColumn(Bytes.toBytes(families[0]), Bytes.toBytes(LINE), Bytes.toBytes(s)); //just put the whole row in for now
            System.err.println(v.hash());
            return put;
        }
    }





}
