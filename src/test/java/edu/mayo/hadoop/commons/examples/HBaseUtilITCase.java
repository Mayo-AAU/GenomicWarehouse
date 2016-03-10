package edu.mayo.hadoop.commons.examples;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseConnector;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by m102417 on 3/10/16.
 *
 * Checks that the utility functions in HBaseUtil are working properly
 *
 */
public class HBaseUtilITCase {

    String tableName = "table";
    String[] columnFamily = {"cf1", "cf2"};


    Configuration configuration;
    HBaseConnector hconnect;
    HBaseUtil hutil;

    @Before
    public void setup() throws Exception {
        configuration = AutoConfigure.getConfiguration();
        hconnect = new HBaseConnector(configuration);
        hutil = new HBaseUtil(hconnect.getConnection());
    }

    @After
    public void teardown() throws IOException {
        hconnect.close();
    }

    @Test
    public void testAddTable() throws Exception {

        //if there was anything in the schema, get rid of it so the tests pass
        hutil.dropAll();

        //get the tables in hbase, it should be zero
        //System.err.println("***************************************");
        //System.err.println(schema.getTables().toString());
        //System.err.println("***************************************");
        assertEquals(0, hutil.getTables().size());

        //create a table, 'users'
        // and put a column family 'info' into the table
        String[] columnFamiles = {"foo","bar"};
        hutil.createTable("users", columnFamiles);

        //get the tables in hbase, it should be now have one
        assertEquals(1, hutil.getTables().size());

        hutil.dropAll();
    }


    //load some data in for testing...
    private void load() throws Exception {
        hutil.put(tableName,"key1", columnFamily[0], "field1", "value1");
        hutil.put(tableName,"key1", columnFamily[1], "field2", "value2");
        hutil.put(tableName,"key2", columnFamily[0], "field2", "value2");

    }

    @Test
    public void testCRUD() throws Exception {
        hutil.dropAll();
        hutil.createTable(tableName,columnFamily);


        //CRUD - Create, Retrieve, Update, Delete

        //Create
        load();


        //Retrieve
        //byte[] b = hutil.get(tableName, columnFamily[0], qualifier, key);
        //System.err.println(Bytes.toString(b));

        //Update
        //Delete


        hutil.dropAll();

    }

    @Test
    public void testScan() throws Exception {
        hutil.dropAll();
        hutil.createTable(tableName,columnFamily);
        load();
        Result[] results = hutil.scan(tableName,columnFamily[0], 1000);
        List<String> pretty = hutil.format(results);
        for(String line : pretty){
            System.err.println(line);
        }
        hutil.dropAll();
    }
}
