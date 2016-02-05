package hbase.util;


import java.io.IOException;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
functional test, you need to have hbase up and running for this to work!
 Look in the sys.properties file and ensure that HBase is at a location that makes sense.
 */
public class HBaseConnectorTestITCase extends TestCase {

    public void testConnect() throws Exception {
        //System.out.println(System.getenv());
        //System.out.println(""+System.getProperty("CLASSPATH"));
        //System.out.println(new java.io.File( "." ).getCanonicalPath());
        //System.setProperty("CLASSPATH", new java.io.File( "." ).getCanonicalPath() + ":" + "/Users/m102417/tools/hbase/hbase-1.1.3/conf" );

        //connect
        HBaseConnector conn = new HBaseConnector();
        Connection con = conn.connect();
        //get schema utilities
        HBaseSchema schema = new HBaseSchema(conn);

        //if there was anything in the schema, get rid of it so the tests pass
        schema.dropAll();

        //get the tables in hbase, it should be zero
        //System.err.println("***************************************");
        //System.err.println(schema.getTables().toString());
        //System.err.println("***************************************");
        assertEquals(0, schema.getTables().size());

        //create a table, 'users'
        // and put a column family 'info' into the table
        String[] columnFamiles = {"foo","bar"};
        schema.createTable("users", columnFamiles);

        //get the tables in hbase, it should be now have one
        assertEquals(1, schema.getTables().size());



        //put in some data

        //drop a table

    }





    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.SNAPPY));

            System.out.print("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println(" Done.");
        }
    }

    public static void modifySchema (Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (admin.tableExists(tableName)) {
                System.out.println("Table does not exist.");
                System.exit(-1);
            }

            HTableDescriptor table = new HTableDescriptor(tableName);

            // Update existing table
            HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
            newColumn.setCompactionCompressionType(Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName, newColumn);

            // Update existing column family
            HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
            existingColumn.setCompactionCompressionType(Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName, table);

            // Disable an existing table
            admin.disableTable(tableName);

            // Delete an existing column family
            admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

            // Delete a table (Need to be disabled first)
            admin.deleteTable(tableName);
        }
    }

}