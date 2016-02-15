package edu.mayo.hadoop.commons.examples.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by m102417 on 2/11/16.
 */
public class MyHBaseDAO {

    public static void insertRecord(Connection c, String tableName, HBaseTestObj obj)
            throws Exception {
        Table table = c.getTable(TableName.valueOf(tableName));
        Put put = createPut(obj);
        table.put(put);
    }

    public static Put createPut(HBaseTestObj obj) {
        Put put = new Put(Bytes.toBytes(obj.getRowKey()));
        put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1"),
                Bytes.toBytes(obj.getData1()));
        put.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("CQ-2"),
                Bytes.toBytes(obj.getData2()));
        return put;
    }
}
