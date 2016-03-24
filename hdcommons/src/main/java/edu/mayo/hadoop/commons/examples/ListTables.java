package edu.mayo.hadoop.commons.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;

/**
 * Created by Daniel Blezek on 2/23/16.
 */
public class ListTables {
    static Logger logger = Logger.getLogger(ListTables.class);

    /**
     * List tables present in HBase.
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) throws Exception {
        logger.info("Making connection to HBase");
        Configuration configuration = AutoConfigure.getConfiguration();
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            try (Admin admin = connection.getAdmin()) {

                // Make a bogus table
                String newTableName = "test-table";
                String colFamily = "test";

                if (!admin.isTableEnabled(TableName.valueOf(newTableName))) {
                    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(newTableName));
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                    admin.createTable(hTableDescriptor);
                }

                TableName[] tableNames = admin.listTableNames();
                logger.info("Found " + tableNames.length + " tables");
                for (TableName tableName : tableNames) {
                    logger.info("Found table: " + tableName.getNameAsString());
                }
            }
        }
    }
}
