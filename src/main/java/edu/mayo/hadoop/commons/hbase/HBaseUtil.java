package edu.mayo.hadoop.commons.hbase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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
     * Interface for puting a individual value into hbase.
     *
     * NOTE, this is a NON BULK interface to put data into HBase.
     * Further information on BULK imports can be found here:
     * http://hbase.apache.org/0.94/book/perf.writing.html#perf.batch.loading
     *
     * Great basic example is here:
     * https://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/
     *
     * Also, make sure you read about auto-commit behavior in hbase!
     */
    public void put(String tableName, String rowKey, String colFamName, String colQualifier, String value) throws Exception {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier), Bytes.toBytes(value));
            table.put(put);
        }
    }

    /**
     * gets a single value from a hbase table
     */
    public byte[] get(String tableName, String rowKey, String colFamName, String colQualifier) throws Exception {
        Result result = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier));
            get.setMaxVersions(1);
            result = table.get(get);
        }
        if(result == null){
            return null;
        }else {
            return result.value();
        }
    }

    /**
     * Raw Scan interface to HBase
     * @param tableName  - The table to get the results from.
     * @param n          - The number of results to return
     * @return
     * @throws IOException
     */
    public Result[] scan(String tableName, String colFamName, int n) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(colFamName));
            ResultScanner scanner = table.getScanner(scan);
            return scanner.next(n);
        }
    }

    /**
     * format a set of results to whatever output the caller specifies (pretty print)
     * @param results
     * @throws IOException
     * Returns formatted lines you can print to the screen or whatever.
     */
    public ArrayList<String> format(Result[] results) throws IOException {
        ArrayList<String> formattedResults = new ArrayList<String>();
        for(Result r: results){
            formattedResults.add(Bytes.toString(r.getRow()));
            NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> map = r.getMap();
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyEntry : map.entrySet())
            {
                NavigableMap<byte[],NavigableMap<Long,byte[]>> columnMap = columnFamilyEntry.getValue();
                formattedResults.add("    " + Bytes.toString(columnFamilyEntry.getKey()));
                for( Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet())
                {
                    NavigableMap<Long,byte[]> cellMap = columnEntry.getValue();
                    for ( Entry<Long, byte[]> cellEntry : cellMap.entrySet()) {
                        formattedResults.add(String.format("        Key : %s, Value : %s", Bytes.toString(columnEntry.getKey()), Bytes.toString(cellEntry.getValue())));
                    }

                }
            }
        }
        return formattedResults;
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

    /**
     * get the tables in a namespace
     */
    public List<String> getTables() throws IOException {
        List<String> tables = new ArrayList<String>();
        Admin admin = connection.getAdmin();
        TableName[] t = admin.listTableNames();
        for(TableName tn : t){
            String tablename = tn.getNameAsString();
            tables.add(tablename);
        }
        admin.close();
        return tables;
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


}
