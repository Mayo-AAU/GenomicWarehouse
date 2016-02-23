package edu.mayo.hadoop.commons.examples;


import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

/**
 * Created by Daniel Blezek on 2/23/16.
 */
public class ListTables {
    static Logger logger = Logger.getLogger(ListTables.class);

    /**
     * List tables present in HBase.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws Exception {
        logger.info("Making connection to HBase");
        Configuration configuration = AutoConfigure.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            try (Admin admin = connection.getAdmin()) {
                TableName[] tableNames = admin.listTableNames();
                logger.info("Found " + tableNames.length + " tables");
                for (TableName tableName : tableNames) {
                    logger.info("Found table: " + tableName.getNameAsString());
                }
            }
        }
    }
}
