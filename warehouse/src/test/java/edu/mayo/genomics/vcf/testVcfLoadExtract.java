package edu.mayo.genomics.vcf;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;

/**
 * to test VcfLoadExtract
 * 
 * @author yxl01
 *
 */
public class testVcfLoadExtract {
    private static SparkConf sconf;
    private static JavaSparkContext sc;
    private static Configuration configuration;
    private static Connection hconnect;
    private static JavaHBaseContext hbaseContext;

    private static HBaseUtil hutil;
    private static boolean runInCluster = false;

    @BeforeClass
    public static void testSetup() throws Exception {
        if (runInCluster) {
            sconf = new SparkConf().setAppName("Junit test vcf load/extract");
            // sconf.set("spark.driver.host", "127.0.0.1");
        } else {
            sconf = new SparkConf().setMaster("local").setAppName("Junit test vcf load/extract");
            sconf.set("spark.driver.host", "127.0.0.1");
        }

        sc = new JavaSparkContext(sconf);
        configuration = AutoConfigure.getConfiguration();
        hconnect = ConnectionFactory.createConnection(configuration);
        hutil = new HBaseUtil(hconnect);
        VcfLoadExtract.createTable(hutil);;
        hbaseContext = new JavaHBaseContext(sc, configuration);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        sc.close();
        sc.stop();
        hconnect.close();
        if (!runInCluster) {
            AutoConfigure.stop();
        }
    }

    @Test
    public void testLoadExtract() throws Exception {
        VcfLoadExtract vLE = new VcfLoadExtract();
        // load 3 gvcf/vcf files into hbase table first
        for (int i = 1; i < 4; i++) {
            vLE.loadVcfFile(hbaseContext, sc, "sample" + i, "src/test/resources/testData/vcf/sample" + i + ".gvcf");
        }

        // extract them out to a single vcf file
        String outFile = "target/extract.vcf";
        FileUtil.fullyDelete(new File(outFile));
        String[] chrList = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20", "21", "22", "23", "24", "25"};
        String[] sampleIDlist = new String[]{"sample3", "sample1", "sample2"};

        vLE.extractVCF(sc, configuration, chrList, sampleIDlist, outFile);

        long lcount = sc.textFile(outFile).count();
        Assert.assertEquals(2391L, lcount);
    }
}
