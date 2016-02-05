package hbase.util;

import edu.mayo.genomics.model.Sample;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Simple Class to Setup the HBase schema
 * Created by m102417 on 1/27/16.
 *
 * note: best documentation on the hbase api is here:
 * https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/
 *
 */
public class HBaseSchema {

    //Tables - CF = ColumnFamily
    //sample (metadata)
    public final String SAMPLE_TABLE = "samples";
    public final String METADATA_CF = "meta";
    //todo:probably need a user table of some sort for ACLs ect in the metadata...
    //variants
    public final String VARIANTS_TABLE = "variants";
    public final String ANNOTATION_CF = "annotation"; //also called INFO in VCF
    public final String SAMPLES_CF = "samples";       //columns to the right of format

    private static final Logger logger = LogManager.getLogger(HBaseSchema.class);

    private HBaseConnector conn;
    public HBaseSchema(HBaseConnector connection){
        conn = connection;
    }

    public void setupSchema() throws Exception {
        String[] vcolumnFamilies = {ANNOTATION_CF, SAMPLES_CF};
        createTable(VARIANTS_TABLE,vcolumnFamilies);
        String[] scolumnFamilies = {METADATA_CF};
        createTable(SAMPLE_TABLE,scolumnFamilies);
    }


    /**
     *
     * @param tableName - the name of the new table
     * @param familys   - the column families in the new table
     * @throws Exception
     */
    public void createTable(String tableName, String[] familys) throws Exception {
        Admin admin = conn.getConnection().getAdmin();
        final TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            //todo: change to logger!
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(table);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            //todo: change to logger!
            System.out.println("create table " + tableName + " ok.");
        }
        admin.close();
    }

    /**
     * string interface to put
     */
    public void put(String tableName, String columnFamily, String rowKey, String value) throws IOException {
        put(tableName,columnFamily,rowKey,Bytes.toBytes(value));
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
     *
     * @param tableName    - the name of the table we are inserting the data to
     * @param columnFamily - the column family that the value belongs e.g. annotation/sample
     * @param rowKey       - the name of the key to store the value under e.g. annotation.SNPEFF_IMPACT/sample.GT; don't use the column family e.g. SNPEFF_IMPACT or GT
     * @param value        - the the raw bytes to store in hbase
     */
    public void put(String tableName, String columnFamily, String rowKey, byte[] value) throws IOException {
        Connection c = conn.getConnection();
        Table table = c.getTable(TableName.valueOf(tableName));
        // Use the table as needed, for a single operation and a single thread
        Put p = new Put(Bytes.toBytes(rowKey));
        p.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(rowKey),value);
        table.put(p);
    }

    /**
     * get the tables in a namespace
     */
    public List<String> getTables() throws IOException {
        List<String> tables = new ArrayList<String>();
        Connection c = conn.getConnection();
        Admin admin = c.getAdmin();
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
        Connection c = conn.getConnection();
        Admin admin = c.getAdmin();
        TableName[] t = admin.listTableNames();
        for(TableName tn : t){
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        admin.close();
    }

    /**
     * takes a sample and stores it to the sample table
     * @param sample
     */
    public void saveSample(Sample sample) throws IOException {
        Connection c = conn.getConnection();
        Table table = c.getTable(TableName.valueOf(SAMPLE_TABLE));
        //if the sample already exists in hbase, you can't load it again
        if(rowExists(SAMPLE_TABLE, sample.getSampleID())){
            throw new RuntimeException("Sample already exists, not loading!  terminating program!");
        }else {
            Put p = new Put(Bytes.toBytes(sample.getSampleID()));
            HashMap<String,String> meta = sample.getMetadata();
            for(String key : meta.keySet()){
                p.addColumn(Bytes.toBytes(METADATA_CF), Bytes.toBytes(key), Bytes.toBytes(meta.get(key)));
            }
            table.put(p);
        }

    }

    public Sample getSample(String id) throws IOException {
        Sample s = new Sample(id);
        HashMap<String,String> meta = new HashMap<String,String>();
        Connection c = conn.getConnection();
        Table t = c.getTable(TableName.valueOf(SAMPLE_TABLE));
        Get get = new Get(Bytes.toBytes(id));
        Result r = t.get(get);
        //metadata
        NavigableMap<byte[], NavigableMap<Long, byte[]>> map = r.getMap().get(Bytes.toBytes(METADATA_CF));
        for( byte[] bkey : map.keySet() ){
            String key = Bytes.toString(bkey);
            //System.out.println(key);
            byte[] bvalue = r.getValue(Bytes.toBytes(METADATA_CF),Bytes.toBytes(key));
            String value = Bytes.toString(bvalue);
            meta.put(key,value);
//            NavigableMap<Long, byte[]> rawmap = map.get(bkey);
//            for(Long l : rawmap.keySet()){
//                byte[] value = rawmap.get(l);
//                System.out.println(l + ":" + Bytes.toString(value));
//            }
        }
        s.setMetadata(meta);
        return s;
    }

    /**
     * simple method that works on a small amount of data to check if a given row exists in hbase
     * @param table
     * @param rowKey
     * @throws IOException
     */
    public boolean rowExists(String table, String rowKey) throws IOException {
        Connection c = conn.getConnection();
        Table t = c.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(rowKey));
        return t.exists(get);
    }


}
