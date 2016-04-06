package edu.mayo.genomics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.List;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import hbase.VCFParser;
import hbase.VCFParserConfig;

public class LoadVCF {
    private static final Logger logger = Logger.getLogger(LoadVCF.class);

    public static void main(String[] args) throws Exception {

        String filename = args[0];
        logger.info("Starting LoadVCF... loading: " + filename);
        Configuration configuration = AutoConfigure.getConfiguration();
        try (Connection hcon = ConnectionFactory.createConnection(configuration)) {
            HBaseUtil hutil = new HBaseUtil(hcon);
            hutil.createTable(VCFParser.Table, VCFParser.families);
            hutil.createTable(VCFParserConfig.getTableName(), VCFParserConfig.getColumnFamily());

            VCFParserConfig config;
            try (InputStream stream = LoadVCF.class.getResourceAsStream("/VCFParser.properties")) {
                config = new VCFParserConfig(stream);
            }

            VCFParser parser = new VCFParser(config);
            parser.parse(filename, VCFParserConfig.getTableName());

            Result[] results = hutil.first(VCFParserConfig.getTableName(), 10);
            List<String> pretty = hutil.format(results);
            int i = 0;
            for (String line : pretty) {
                System.out.println(line);
                i++;
            }
        }
        System.exit(0);
        // parser.shutdown();

        // connect
        // HBaseConnector conn = new HBaseConnector();
        // conn.connect();
        // //get schema utilities
        // HBaseSchema schema = new HBaseSchema(conn);
        // //connect to hbase and setup the schema
        // schema.dropAll();
        // schema.setupSchema();
        // //configure the parser
        // VCFParserConfig config = new VCFParserConfig();
        // VCFParser parser = new VCFParser(config, schema);
        //
        // //parse the file
        // parser.parse("/data/VCF/NA_1424005550.gvcf.gz");//todo: need to wire
        // in the CLI to enable command line loading
        // conn.close();
    }

}
