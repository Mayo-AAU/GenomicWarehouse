package edu.mayo.genomics.vcf;

import java.io.File;

import org.apache.hadoop.fs.FileUtil;
import org.junit.Test;

/**
 * to test VcfLoadExtract
 * 
 * @author yxl01
 *
 */
public class testVcfLoadExtract {
	@Test
    public void testLoadExtract () throws Exception {
    	VcfLoadExtract vLE = new VcfLoadExtract();
    	boolean runInCluster = false;
    	vLE.init(runInCluster);
    	
    	try {
		    // load 3 gvcf/vcf files into hbase table first
		    for (int i=1; i < 4; i++) {
		    	vLE.loadVcfFile ("sample" + i, "src/test/resources/testData/vcf/sample" + i + ".gvcf");
		    }
		    
		    // extract them out to a single vcf file
			FileUtil.fullyDelete(new File("src/test/resources/testData/vcf/extract.vcf"));
			String[] chrList = new String[]{"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25"};
			String[] sampleIDlist = new String[]{"sample1", "sample2", "sample3"};
			
		    vLE.extractVCF (chrList, sampleIDlist, "src/test/resources/testData/vcf/extract.vcf");
    	}
    	finally {
    		vLE.stop(runInCluster);
    	}
    }
}
