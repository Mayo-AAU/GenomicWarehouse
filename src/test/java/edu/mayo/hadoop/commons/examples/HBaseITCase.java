package edu.mayo.hadoop.commons.examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;

public class HBaseITCase {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(HBaseITCase.class);

    @AfterClass
    public static void tearDown() throws Exception {
        AutoConfigure.stop();
    }

    @Test
    public void running() throws Exception {
        Configuration configuration = AutoConfigure.getConfiguration();
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);
        } catch (MasterNotRunningException mnre) {
            fail("Master is not running: " + mnre.getMessage() + "\n");
            throw mnre;
        } catch (ZooKeeperConnectionException zkce) {
            fail("ZooKeeper Connection exception: " + zkce.getMessage());
            throw zkce;
        } catch (ServiceException se) {
            fail("Protobuf exception: " + se.getLocalizedMessage());
            throw se;
        } catch (IOException e) {
            fail("IO exception: " + e.getLocalizedMessage());
            throw e;
        }
    }

    @Test
    public void testHbaseLocalCluster() throws Exception {

        LOG.info("Establishing a connection with HBase");
        Configuration configuration = AutoConfigure.getConfiguration();
        try (Connection hcon = ConnectionFactory.createConnection(configuration)) {
            HBaseUtil hutil = new HBaseUtil(hcon);

            LOG.info("Drop Tables in case things did not cleanup correctly in the past");
            hutil.dropAll();

            String tableName = "hbase_test_table";
            String colFamName = "cf1";
            String colQualiferName = "ca1";
            Integer numRowsToPut = 50;

            LOG.info("HBASE: Creating table {} with column family {}", tableName, colFamName);
            createHbaseTable(hcon, tableName, colFamName, configuration);

            LOG.info("HBASE: Populate the table with {} rows.", numRowsToPut);
            for (int i = 0; i < numRowsToPut; i++) {
                putRow(hcon, tableName, colFamName, String.valueOf(i), colQualiferName, "row_" + i, configuration);
            }

            LOG.info("HBASE: Fetching and comparing the results");
            for (int i = 0; i < numRowsToPut; i++) {
                Result result = getRow(hcon, tableName, colFamName, String.valueOf(i), colQualiferName, configuration);
                assertNotNull(result);
                assertEquals("row_" + i, new String(result.value()));
            }

            LOG.info("Test complete, dropping schema!");
            hutil.dropAll();
        }
    }

    private static void createHbaseTable(Connection connection, String tableName, String colFamily,
            Configuration configuration) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
        }
    }

    private static void putRow(Connection connection, String tableName, String colFamName, String rowKey,
            String colQualifier, String value, Configuration configuration) throws Exception {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier), Bytes.toBytes(value));
            table.put(put);
        }
    }

    private static Result getRow(Connection connection, String tableName, String colFamName, String rowKey,
            String colQualifier, Configuration configuration) throws Exception {
        Result result = null;
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            // HTable table = new HTable(configuration, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(colQualifier));
            get.setMaxVersions(1);
            result = table.get(get);
        }
        return result;
    }

}