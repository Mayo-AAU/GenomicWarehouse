package edu.mayo.hadoop.commons.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by m102417 on 2/12/16.
 *
 * Basic utility functions for interacting with HBASE.
 *
 */
public class HBaseUtil {

    private HBaseConnector conn;

    public HBaseUtil(HBaseConnector con) {
        conn = con;
    }

    /**
     * drop all tables in the schema
     */
    public void dropAll() throws IOException {
        Connection c = conn.getConnection();
        Admin admin = c.getAdmin();
        TableName[] t = admin.listTableNames();
        for (TableName tn : t) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        admin.close();
    }

}
