package hbase;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.lang.ClassLoader.*;

/**
 * Created by m102417 on 2/3/16.
 */
public class VCFParserConfig {

    /**
     * configuration variables for SPARK
     */
    //keys
    public static final String SPARK_DRIVER_KEY = "spark.driver.host";
    //properties
    public static final String SPARK_MASTER = "spark-master";
    public static final String SPARK_APP_NAME = "spark-app";
    public static final String SPARK_HOST_IP = "spark-host-ip";
    public static final String MAX_HEADER = "max-header";

    //todo: put this out in the properties file
    private static final String tableName = "variants";
    private static final String[] columnFamily = {"base","annotation", "samples"};

    /**
     * configuration variables
     */
    private boolean includeNON_REF = false;

    Properties props = null;
    Configuration configuration;

    public VCFParserConfig(String properties) throws Exception {
        props = parseProperties(properties);
        configuration = AutoConfigure.getConfiguration();
    }

    public Properties parseProperties(String propertiesFile) throws IOException {
        Properties props = new Properties();
        try (InputStream stream = new FileInputStream(new File(propertiesFile))) {
            props.putAll(loadPropertiesStream(stream));
        }
        return props;
    }

    public static Properties loadPropertiesStream(InputStream input) throws IOException {
        Properties prop = new Properties();
        prop.load(input);
        return prop;
    }

    public String getProperty(String property){
        return props.getProperty(property);
    }

    public void setProperty(String key, String value){
        props.setProperty(key,value);
    }


    /**
     * method to determine if we should include a line based on the configuration setup
     * @param line
     * @return
     */
    public boolean includeline(String line){
        //todo: placeholder for all of the loading options that will inevitably follow (e.g. throw out problematic lines ect)
//        if(includeNON_REF && line.contains(NON_REF)){  //todo: we may still want to load data from a line with non-ref in it; need to check with steve
//            return false;
//        }
        return true;
    }


    /**
     * get the maximum number of lines that the header can be
     * @return
     */
    public int getMaxHeader(){
        String sheader = getProperty(MAX_HEADER);
        return new Integer(sheader);
    }

    /**
     * get back the configuration for Spark
     * @return
     */
    public SparkConf getSparkConfiguration(){
        //todo: use Dan's spark auto configure from hadoopcommons
        SparkConf sconf = new SparkConf().setMaster("local").setAppName("Spark-Hbase Connector");
        sconf.set("spark.driver.host", "127.0.0.1");
        return sconf;

    }


    public Configuration getHBaseConfig() {
        return configuration;
    }


    public static String getTableName() {
        return tableName;
    }

    public static String[] getColumnFamily() {
        return columnFamily;
    }

    public Properties getProps() {
        return props;
    }
}
