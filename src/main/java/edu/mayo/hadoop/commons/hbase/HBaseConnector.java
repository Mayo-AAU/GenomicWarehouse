package edu.mayo.hadoop.commons.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by m102417 on 1/26/16.
 * Connects the code to HBase via the hbase client.
 */
public class HBaseConnector {

    private Configuration config = null;
    private Connection connection = null;

    public HBaseConnector(Configuration config) throws IOException {
        connect(config);
    }

    /**
     * when we have a configuration, but not a connection, this constructor will make a new connection
     */
    public Connection connect(Configuration config) throws IOException {
        this.config = config;
        connection = ConnectionFactory.createConnection(config);
        return connection;
    }

    /**
     * instead of using the classpath to find the hbase configuration files, use this constructor
     * to explicity set the configuration on hbase (for example if you want to run local tests).
     * @param c
     * @return
     */
    public Connection connect(Connection c) throws IOException {
        config = c.getConfiguration();
        connection = ConnectionFactory.createConnection(config);
        return connection;
    }

    /**
     * uses the HADOOP envoronment variables to infer the configuration.
     * @return
     * @throws IOException
     */
    public Connection connect() throws IOException {
        // You need a configuration object to tell the client where to connect.
        // When you create a HBaseConfiguration, it reads in whatever you've set
        // into your hbase-site.xml and in hbase-default.xml, as long as these
        // can be found on the CLASSPATH
        config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        //config.addResource(new Path(System.getenv("HBASE_HOME"), "/conf/hbase-site.xml"));
        //config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        //config.addResource("~/tools/hd/hadoop/hadoop-2.7.2/etc/hadoop/core-site.xml");

        // Next you need a Connection to the cluster. Create one. When done with it,
        // close it. A try/finally is a good way to ensure it gets closed or use
        // the jdk7 idiom, try-with-resources: see
        // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
        //
        // Connections are heavyweight.  Create one once and keep it around. From a Connection
        // you get a Table instance to access Tables, an Admin instance to administer the cluster,
        // and RegionLocator to find where regions are out on the cluster. As opposed to Connections,
        // Table, Admin and RegionLocator instances are lightweight; create as you need them and then
        // close when done.
        //
        connection = ConnectionFactory.createConnection(config);
        return connection;
    }


    public Configuration getConfig() {
        return config;
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void close() throws IOException {
        connection.close();
    }
}

