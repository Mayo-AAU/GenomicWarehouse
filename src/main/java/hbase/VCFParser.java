package hbase;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.genomics.model.Variant;
import edu.mayo.pipes.UNIX.CatPipe;
import hbase.util.HBaseConnector;
import hbase.util.HBaseSchema;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by m102417 on 1/26/16.
 *
 *  This is a simple class that parses a single sample VCF file and pushes it to HBase
 *
 */
public class VCFParser {

    private VCFParserConfig config;
    private HBaseSchema schema;

    /**
     * constructing a VCFParser requires a parser config object to ensure that
     * all of the options are accounted for.
     * @param config
     */
    public VCFParser(VCFParserConfig config, HBaseSchema schema){
        this.config = config;
        this.schema = schema;
    }

    /**
     * Takes a VCF file and pushes it to hbase
     * @param inputVCF - the input path for the vcf file
     */
    public void parse(String inputVCF) throws ParseException, IOException {
        ArrayList<String> headerlines = new ArrayList<String>();
        Pipeline bgzipReader = new Pipeline(new CatPipe());
        bgzipReader.setStarts(Arrays.asList(inputVCF));
        int count = 0;
        while(bgzipReader.hasNext()){
            String line = (String) bgzipReader.next();
            if(line.startsWith("#")){
                headerlines.add(line);
                System.out.println(line);
            }else { //data line

                if(config.includeline(line)) {
                    Variant v = new Variant(line);
                    System.out.println(v.hash());
                    //schema.put(schema.VARIANTS_TABLE, schema.ANNOTATION_CF, "linecontents", line);

                    //System.out.println(line);
                }
            }
            if(count > 1000) break;
            count++;
        }

    }

    public static void main(String[] args) throws Exception {
        //connect
//        HBaseConnector conn = new HBaseConnector();
//        conn.connect();
//        //get schema utilities
//        HBaseSchema schema = new HBaseSchema(conn);
//        //connect to hbase and setup the schema
//        schema.dropAll();
//        schema.setupSchema();
//        //configure the parser
//        VCFParserConfig config = new VCFParserConfig();
//        VCFParser parser = new VCFParser(config, schema);
//
//        //parse the file
//        parser.parse("/data/VCF/NA_1424005550.gvcf.gz");//todo: need to wire in the CLI to enable command line loading
//        conn.close();
    }


}
