import edu.mayo.genomics.model.Sample;
import hbase.util.HBaseConnector;
import hbase.util.HBaseSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Created by m102417 on 2/3/16.
 */
public class testSampleLoadITCase {

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
            HBaseConnector conn = new HBaseConnector();
            conn.connect();
            HBaseSchema schema = new HBaseSchema(conn);
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
