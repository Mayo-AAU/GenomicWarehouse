import edu.mayo.genomics.model.Sample;
import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseConnector;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import hbase.util.HBaseSchema;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by m102417 on 2/3/16.
 */
public class testSampleLoadITCase {

    static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(testSampleLoadITCase.class);

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
    public void testLoadMetadata() throws Exception {
        String id = "MySample";
        //first just do a simple file load...
        Sample sample = new Sample(id,"src/test/resources/metadata.example");
        assertEquals(id, sample.getSampleID());
        HashMap<String,String> meta = sample.getMetadata();
        assertEquals(8, meta.size());
        assertEquals(meta.get("sample"),"MySample");
        assertEquals(meta.get("kit_id"),"SimplexoCustomCapture");
        assertEquals(meta.get("study_id"),"Simplexo");
        assertEquals(meta.get("tumorType"),"Breast");
        assertEquals(meta.get("tumor"),"true");
        assertEquals(meta.get("status"),"case");
        assertEquals(meta.get("age"),"35");
        assertEquals(meta.get("patient_id"),"1290344");

        //now push the data to hbase and make sure it is there correctly
        HBaseSchema schema = new HBaseSchema(hconnect);
        schema.dropAll();
        schema.setupSchema();
        schema.saveSample(sample);

        //now get the sample out and ensure it is correct
        Sample s = schema.getSample(id);
        meta = s.getMetadata();
        assertEquals(sample.getSampleID(), s.getSampleID());
        assertEquals(meta.get("sample"),"MySample");
        assertEquals(meta.get("kit_id"),"SimplexoCustomCapture");
        assertEquals(meta.get("study_id"),"Simplexo");
        assertEquals(meta.get("tumorType"),"Breast");
        assertEquals(meta.get("tumor"),"true");
        assertEquals(meta.get("status"),"case");
        assertEquals(meta.get("age"),"35");
        assertEquals(meta.get("patient_id"),"1290344");

    }


}
