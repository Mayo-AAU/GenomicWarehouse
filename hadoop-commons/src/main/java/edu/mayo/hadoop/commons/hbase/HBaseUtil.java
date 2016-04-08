package edu.mayo.hadoop.commons.hbase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
     * Takes a shallow JSON structure, converts it into a map and then puts it in hbase
     * this method CAN NOT handle deeply nested JSON, it will NOT know what to do with it
     * The json structure has to be like this:
     *
     * {
     *     columnFamily1 : {
     *         key1 : value1,
     *         key2 : value2
     *     }
     *     columnFamily2 : {
     *         key1 : value1,
     *         key2 : value2
     *     }
     * }
     *
     * Timestamps will be automatically generated!
     *
     * @param tableName
     * @param rowKey     -
     * @param json       - the json data
     */
    public void putJSON(String tableName, String rowKey, String json) throws ParseException, IOException {
        long idx = new Long(1);
        Map<byte[],Map<byte[],Map<Long,byte[]>>> map = new  HashMap<>();
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(json);
        //go through the column families
        for(Object cf : jsonObject.keySet()){
            Map<byte[],Map<Long,byte[]>> familyHash = new HashMap<>();
            String columnFamily = (String) cf;
            JSONObject data = (JSONObject) jsonObject.get(columnFamily);
            for(Object okey: ((JSONObject)data).keySet() ){
                String key = (String) okey;
                Object value = data.get(key);
                if(value != null){
                    Map<Long,byte[]> pair = new HashMap<>();
                    //System.err.println(value.toString());
                    pair.put(idx, toBytes(value));
                    familyHash.put(Bytes.toBytes(key),pair);
                }//do nothing, we can't put null data into the hash
            }
            map.put(Bytes.toBytes(columnFamily), familyHash);
        }
        putMap(tableName, rowKey, map, false);

    }

    /**
     * slower version that checks if it is a String integer or double before converting it to bytes
     * @param o
     * @return
     */
    public byte[] toBytes(Object o){
        //try integer first
        try {
            Integer v = Integer.parseInt(o.toString());
            return Bytes.toBytes(v);
        }catch(Exception e){
            ; //do nothing, it was not an integer
        }
        //ok then try Double
        try {
            Double v = Double.parseDouble(o.toString());
            return Bytes.toBytes(v);
        }catch(Exception e){
            ; //do nothing, it was not an integer
        }
        //Last, just treat it as a string...
        return Bytes.toBytes(o.toString());
    }



    /**
     * Raw interface to generating puts into hbase.
     * @param tableName
     * @param rowKey
     * @param map
     * @param useTimestamps - use the timestamps provided in the 'Long' bit of the datastructure versus generate the timestamps.
     * @throws IOException
     */
    public void putMap(String tableName, String rowKey, Map<byte[],Map<byte[],Map<Long,byte[]>>> map, boolean useTimestamps) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Entry<byte[], Map<byte[], Map<Long, byte[]>>> columnFamilyEntry : map.entrySet()){
                //String columnFamily = Bytes.toString(columnFamilyEntry.getKey());
                Map<byte[],Map<Long,byte[]>> columnMap = columnFamilyEntry.getValue();
                for( Entry<byte[], Map<Long, byte[]>> columnEntry : columnMap.entrySet()) {
                    Map<Long,byte[]> cellMap = columnEntry.getValue();
                    for ( Entry<Long, byte[]> cellEntry : cellMap.entrySet()) {
                        if(useTimestamps){
                            put.addColumn(columnFamilyEntry.getKey(), columnEntry.getKey(), cellEntry.getKey(), cellEntry.getValue());
                        }else {
                            put.addColumn(columnFamilyEntry.getKey(), columnEntry.getKey(), cellEntry.getValue());
                        }
                    }
                }
            table.put(put);
            }
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
     * this function deletes all data for a given row
     * @param tableName
     * @param key
     */
    public void deleteRow(String tableName, String key) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(toBytes(key));
            table.delete(delete);
        }
    }

    /**
     * Raw Scan interface to HBase, just get the first n rows
     * @param tableName  - The table to get the results from.
     * @param n          - The number of results to return
     * @return
     * @throws IOException
     */
    public Result[] first(String tableName, int n) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            return scanner.next(n);
        }
    }
    
    public Result[] firstWitFilter (String tableName, String prefix, int n) throws IOException {
    	try (Table table = connection.getTable(TableName.valueOf(tableName))) {
        	Filter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
        	Scan scan = new Scan (Bytes.toBytes(prefix));
        	scan.setFilter(prefixFilter);
        	ResultScanner rscan = table.getScanner(scan);
        	return rscan.next(n);
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
            //key for the row
            formattedResults.add(Bytes.toString(r.getRow()));
            NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> map = r.getMap();
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyEntry : map.entrySet())
            {
                //column family
                NavigableMap<byte[],NavigableMap<Long,byte[]>> columnMap = columnFamilyEntry.getValue();
                formattedResults.add("    " + Bytes.toString(columnFamilyEntry.getKey()));
                for( Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet())
                {
                    NavigableMap<Long,byte[]> cellMap = columnEntry.getValue();
                    for ( Entry<Long, byte[]> cellEntry : cellMap.entrySet()) {
                        //data values
                        formattedResults.add(String.format("        Key : %s, Value : %s, NumericalValue : %s",
                                Bytes.toString(columnEntry.getKey()),
                                Bytes.toString(cellEntry.getValue()),
                                stringify(cellEntry.getValue())
                        ));
                    }

                }
            }
        }
        return formattedResults;
    }

    //for display only
    private String stringify(byte[] bytes){
        try{
            Double d = Bytes.toDouble(bytes);
            return d.toString();
        }catch (Exception e){
            //no big thing try integer!
        }
        try{
            Integer i = Bytes.toInt(bytes);
            return i.toString();
        }catch (Exception e){
            //no big thing its a stiring
        }
        return Bytes.toString(bytes);
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

    /**
     * drop the specified table if it exists
     * @param tableName
     * @throws IOException
     */
    public void dropTable (String tableName) throws IOException {
    	TableName tbName = TableName.valueOf(tableName);
        try (Admin admin = connection.getAdmin()) {
        	if (admin.tableExists(tbName)) {
                admin.disableTable(tbName);
                admin.deleteTable(tbName);
            }
        	admin.close();
        }
    }

    
    /**
     * simple method that works on a small amount of data to check if a given row exists in hbase
     * @param table
     * @param rowKey
     * @throws IOException
     */
    public boolean rowExists(String table, String rowKey) throws IOException {
        Table t = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(rowKey));
        return t.exists(get);
    }


}
