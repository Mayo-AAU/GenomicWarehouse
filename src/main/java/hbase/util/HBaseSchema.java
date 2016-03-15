package hbase.util;

import edu.mayo.genomics.model.Sample;
import edu.mayo.hadoop.commons.hbase.HBaseConnector;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
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

    HBaseUtil util;

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
        util = new HBaseUtil(connection.getConnection());
    }

    public void setupSchema() throws Exception {
        String[] vcolumnFamilies = {ANNOTATION_CF, SAMPLES_CF};
        util.createTable(VARIANTS_TABLE, vcolumnFamilies);
        String[] scolumnFamilies = {METADATA_CF};
        util.createTable(SAMPLE_TABLE, scolumnFamilies);
    }


    /**
     * takes a sample and stores it to the sample table
     * @param sample
     */
    public void saveSample(Sample sample) throws IOException {
        Connection c = conn.getConnection();
        Table table = c.getTable(TableName.valueOf(SAMPLE_TABLE));
        //if the sample already exists in hbase, you can't load it again
        if(util.rowExists(SAMPLE_TABLE, sample.getSampleID())){
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


    public void dropAll() throws IOException {
        util.dropAll();
    }
}
