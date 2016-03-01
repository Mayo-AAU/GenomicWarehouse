package edu.mayo.hadoop.commons.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;

/**
 * Created by m102417 on 2/12/16.
 *
 * Basic utility functions for interacting with HBASE.
 *
 */
public class HBaseUtil {

    static Logger logger = Logger.getLogger(HBaseUtil.class);

    private Connection connection;

    public HBaseUtil(Connection con) {
        this.connection = con;
    }

    /**
     * drop all tables in the schema
     */
    public void dropAll() throws IOException {
        try (Admin admin = connection.getAdmin()) {
            TableName[] t = admin.listTableNames();
            for (TableName tn : t) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
        }
    }

    /**
     *
     * @param tableName - the name of the new table
     * @param familys   - the column families in the new table
     * @throws Exception
     */
    public void createTable(String tableName, String[] familys) throws Exception {
        Admin admin = connection.getAdmin();
        final TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            logger.warn("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(table);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            logger.info("create table " + tableName + " ok.");
        }
        admin.close();
    }

}
