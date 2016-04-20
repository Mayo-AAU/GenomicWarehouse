package edu.mayo.genomics.vcf;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import edu.mayo.hadoop.commons.hbase.AutoConfigure;
import edu.mayo.hadoop.commons.hbase.HBaseUtil;
import scala.Tuple2;

/**
 * gvcf file loader and vcf extraction.
 * 
 * It loads gvcf files listed and performs vcf extraction on the samples and chromosomes specified.
 * gvcf file content is stored in hbase table using chr\tsmapleid as the key and the content in a column gvcf of column family gvcfCF as a text string from gvcf file.
 * 
 * @author yxl01
 *
 */
public class VcfLoadExtract implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String tableName = "GvcfTable";
    private static String gvcfCol = "gvcf";
    private static String gvcfCF = "gvcfCF";
    private static byte[] gvcfColBytes = Bytes.toBytes(gvcfCol);
    private static byte[] gvcfCFBytes = Bytes.toBytes(gvcfCF);
	private static int formatCol = 8;
	private static int sampleCol = 9;
	private static int infoCol = 7;
	private static int posCol = 1;
    private static final Logger logger = Logger.getLogger(VcfLoadExtract.class);
    
    // this sampleIdList is to be used to pass sample ids to the executor
    private String[] vcfExtractSampleIdList; 

    /**
     * modify this method to change bucket of current chr to specific ranges within a chr to achieve more parallelism.
     * 
     * This change must be reflected in the vcf extraction.
     * 
     * 
     * @param line
     * @return
     */
    private static String getBucketID(String line) {
    	return line.split("\t")[0];
    }

    public static void createTable (HBaseUtil hutil) throws Exception {
        hutil.createTable(VcfLoadExtract.tableName, new String[] { gvcfCF});
    }
    
    public void loadVcfFile (JavaHBaseContext hbaseContext, JavaSparkContext sc, String sampleID, String filePath)  throws Exception {
    	JavaRDD<String> rdd = sc.textFile(filePath)
    			.filter(s -> !s.startsWith("#") )
    			.mapToPair(line -> new Tuple2<String, String> (getBucketID(line), line))
    			.reduceByKey((x,y) -> x+"\n" + y)
    			.map(x -> x._1 + "##" + sampleID + "##" + x._2);
         hbaseContext.bulkPut(rdd, TableName.valueOf(tableName), new PutVcf());
    }
    
    
    private static class PutVcf implements Function<String, Put> {
        private static final long serialVersionUID = 1L;
        @Override
        public Put call (String v) throws Exception {
        	String [] list = v.split("##");
        	String key = list[0] + "\t" + list[1];
        	Put put = new Put(Bytes.toBytes(key));
            put.addColumn(gvcfCFBytes, gvcfColBytes, Bytes.toBytes(list[2]));
        	return put;
        }
    }

//    public static class VcfPartitioner extends Partitioner {
//    	private static final long serialVersionUID= 1L;
//    	private int numParts;
//    	public VcfPartitioner() {
//    		
//    	}
//    	
//    	public VcfPartitioner(int num) {
//    		this.numParts = num;
//    	}
//
//		@Override
//		public int getPartition(Object key) {
//			String chr = key.toString();
//			System.err.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx part key is: " + chr);
//			
//			String[] keyList = chr.split("\t");
//			if (keyList.length > 1) {
//				chr = keyList[1];
//			}
//			chr = chr.toLowerCase();
//			if (chr.startsWith("chr")) {
//				chr = chr.substring(3);
//			}
//			else if (chr.equals("chrX")) return 23;
//			else if (chr.equals("chrY")) return 24;
//			else if (chr.equals("chrMT")) return 25;
//			return Integer.parseInt(chr);
//		}
//
//		@Override
//		public int numPartitions() {
//			return this.numParts;
//		}
//		
//		
//		@Override
//		public boolean equals(Object other) {
//			if (other instanceof VcfPartitioner) {
//				return ((VcfPartitioner) other).numParts == this.numParts;
//			}
//			else {
//				return false;
//			}
//		}
//		
//    }
    
//    public static class ChrSampleComparator implements Serializable, Comparator <String> {
//    	private static final long serialVersionUID= 1L;
//
//		@Override
//		public int compare(String o1, String o2) {
//			String [] list1 = o1.split("\t");
//			String [] list2 = o2.split("\t");
//			if (list1[0].equals(list2[0])) {
//				if (list1.length > 1 && list2.length > 1) {
//					return list1[1].compareTo(list2[1]);
//				}
//				else return 0;
//			}
//			return list1[0].compareTo(list2[0]);
//		}
//    }
    
    public void extractVCF (JavaSparkContext sc, Configuration configuration, String [] chrList, String[] sampleIdList, String outFilePath) throws Exception {
    	this.vcfExtractSampleIdList = sampleIdList;
    	StringBuffer sampleHeader = new StringBuffer();
    	for (String s: sampleIdList) {
    		sampleHeader.append(s).append("\t");
    	}
        JavaRDD<String> chrListRdd = sc.parallelize(Arrays.asList(chrList));
        JavaRDD<String> sListRdd = sc.parallelize(Arrays.asList(sampleIdList));
        JavaRDD<String> rdd = chrListRdd.cartesian(sListRdd).map(x -> x._1 + "\t" + x._2);
        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
        
//        JavaRDD<String> headerRdd = sc.parallelize(Arrays.asList(new String[] {"##fileformat=VCFv4.1\n##FILTER=<ID=LowQual,Description=\"Low quality\">\n##FORMAT=<ID=AD,Number=.,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed\">\n##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Approximate read depth (reads with MQ=255 or with bad mates are filtered)\">\n##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype Quality\">\n##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n##FORMAT=<ID=PL,Number=G,Type=Integer,Description=\"Normalized, Phred-scaled likelihoods for genotypes as defined in the VCF specification\">"
//        		+ "##INFO=<ID=AC,Number=A,Type=Integer,Description=\"Allele count in genotypes, for each ALT allele, in the same order as listed\">\n"
//        		+ "##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">\n"
//        		+ "##INFO=<ID=AN,Number=1,Type=Integer,Description=\"Total number of alleles in called genotypes\">\n"
//        		+ "#CHROM \tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t"+ sampleHeader}));
//        
//        
//        // for now we have one row per sample per chr.
//        long startMillis = System.currentTimeMillis();
//        JavaRDD <String> rdd2A = hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetGvcf(), new ResultGvcf())
//        		.filter(x -> x!=null)
//        		.mapToPair( x -> new Tuple2<String,String>(x.substring(0, x.indexOf("##")).split("\t")[0], x))
//				.reduceByKey((x,y)-> x + "@@" + y)
//				.map(x -> mergeGvcf(x._2.split("@@")));
//        headerRdd.union(rdd2A).saveAsTextFile(outFilePath);
//        System.err.println("Extract A millis: " + (System.currentTimeMillis()-startMillis));

        JavaRDD<String> headerRdd2 = sc.parallelize(Arrays.asList(new String[] {"##fileformat=VCFv4.1\n##FILTER=<ID=LowQual,Description=\"Low quality\">\n##FORMAT=<ID=AD,Number=.,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed\">\n##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Approximate read depth (reads with MQ=255 or with bad mates are filtered)\">\n##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype Quality\">\n##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n##FORMAT=<ID=PL,Number=G,Type=Integer,Description=\"Normalized, Phred-scaled likelihoods for genotypes as defined in the VCF specification\">"
        		+ "##INFO=<ID=AC,Number=A,Type=Integer,Description=\"Allele count in genotypes, for each ALT allele, in the same order as listed\">\n"
        		+ "##INFO=<ID=AF,Number=A,Type=Float,Description=\"Allele Frequency, for each ALT allele, in the same order as listed\">\n"
        		+ "##INFO=<ID=AN,Number=1,Type=Integer,Description=\"Total number of alleles in called genotypes\">\n"
        		+ "#CHROM \tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t"+ sampleHeader}));
        
        long startMillis = System.currentTimeMillis();
        JavaRDD <String> rdd2B = hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetGvcf(), new ResultGvcf())
        		.filter(x -> x!=null)
        		.mapToPair( x -> new Tuple2<String,String>(x.substring(0, x.indexOf("##")).split("\t")[0], x))
				.groupByKey()
				.map(x -> mergeGvcf(x))
				.sortBy(new SortVcf(), true, 1);

        headerRdd2.union(rdd2B).saveAsTextFile(outFilePath);
        System.err.println("Extract B millis: " + (System.currentTimeMillis()-startMillis));

        
//        List<String> resultList = rdd2.take(10);
//        for (int i=0; i< resultList.size(); i++) {
//        	if (resultList.get(i)!=null) {
//        		System.err.println(resultList.get(i));
//        	}
//        }
    }

    
    private String mergeGvcf (String[] lineList) throws Exception {
    	StringBuilder retBuf = new StringBuilder();
    	int sampleNum = lineList.length;
    	VcfLine [] sampleIdxList = new VcfLine [sampleNum];
    	VcfLine [] sampleIdxNextList = new VcfLine [sampleNum];
    	
    	List<String[]> sampleGvcfLineList = new java.util.ArrayList<String []>(sampleNum);
    	for (int sIdx = 0; sIdx < sampleNum; sIdx++) {
    		String [] gvcfLine = lineList[sIdx].split("\n");
    		sampleGvcfLineList.add(gvcfLine);
    		sampleIdxList[sIdx] = new VcfLine (gvcfLine[0], 0);
    		if (gvcfLine.length > 1) {
    			sampleIdxNextList[sIdx] = new VcfLine (gvcfLine[1], 1);
    		}
    	}
    	
    	while (true) {
    		int curSampleIdx = selectSample(sampleGvcfLineList, sampleIdxList);
    		if (curSampleIdx < 0) break;
    		genVcfLine(sampleGvcfLineList, sampleIdxList, curSampleIdx, sampleIdxNextList, retBuf);
    		
    		// prepare next line for samples that have been advanced one line by genVcfLine()
    		for (int sIdx = 0; sIdx < sampleNum; sIdx++) {
    			VcfLine nextLine = sampleIdxNextList [sIdx];
    			VcfLine curLine = sampleIdxList[sIdx];
    			if (nextLine == null && curLine != null) {
    				String[] curGvcfLine = sampleGvcfLineList.get(sIdx);
    				int nextLineNum = curLine.lineIdx + 1;
    				if (nextLineNum < curGvcfLine.length ) {
    	    			sampleIdxNextList[sIdx] = new VcfLine (curGvcfLine[nextLineNum], nextLineNum);
    				}
    			}
    		}
    	}
    	
    	return retBuf.toString();
    }
    
    private String mergeGvcf (Tuple2 <String, Iterable<String>> lineTuple) throws Exception {
    	StringBuilder retBuf = new StringBuilder();
    	List<String> lineList = new java.util.ArrayList<String>();
    	Iterator<String> it = lineTuple._2.iterator();
    	while (it.hasNext()) {
    		lineList.add(it.next());
    	}
    	int sampleNum = lineList.size();
    	VcfLine [] sampleIdxList = new VcfLine [sampleNum];
    	VcfLine [] sampleIdxNextList = new VcfLine [sampleNum];
    	
    	List<String[]> sampleGvcfLineList = new java.util.ArrayList<String []>(sampleNum);
    	for (int sIdx = 0; sIdx < sampleNum; sIdx++) {
    		String [] gvcfLine = lineList.get(sIdx).split("\n");
    		sampleGvcfLineList.add(gvcfLine);
    		sampleIdxList[sIdx] = new VcfLine (gvcfLine[0], 0);
    		if (gvcfLine.length > 1) {
    			sampleIdxNextList[sIdx] = new VcfLine (gvcfLine[1], 1);
    		}
    	}
    	
    	while (true) {
    		int curSampleIdx = selectSample(sampleGvcfLineList, sampleIdxList);
    		if (curSampleIdx < 0) break;
    		genVcfLine(sampleGvcfLineList, sampleIdxList, curSampleIdx, sampleIdxNextList, retBuf);
    		
    		// prepare next line for samples that have been advanced one line by genVcfLine()
    		for (int sIdx = 0; sIdx < sampleNum; sIdx++) {
    			VcfLine nextLine = sampleIdxNextList [sIdx];
    			VcfLine curLine = sampleIdxList[sIdx];
    			if (nextLine == null && curLine != null) {
    				String[] curGvcfLine = sampleGvcfLineList.get(sIdx);
    				int nextLineNum = curLine.lineIdx + 1;
    				if (nextLineNum < curGvcfLine.length ) {
    	    			sampleIdxNextList[sIdx] = new VcfLine (curGvcfLine[nextLineNum], nextLineNum);
    				}
    			}
    		}
    	}
    	
    	return retBuf.toString();
    }
    
    private static class VcfLine {
    	public String sampleId = "?";
    	public int lineIdx;
    	public long pos;
    	public long end; // -1 indicates contains no end
    	public String[] pieces;
    	public List<String> formatFields = new java.util.ArrayList<String>();
    	public Map<String,String> formatList = new java.util.HashMap<String,String>();
    	
    	public VcfLine (String line, int lineNum) throws Exception {
    		this.lineIdx = lineNum;
    		int idx0 = line.indexOf("##");
    		if (idx0 > 0) {
    			this.sampleId = line.substring(0, idx0);
    			line = line.substring(idx0+2);
    		}
    		this.pieces = line.split("\t");
    		try {
    			this.pos = Long.parseLong(this.pieces[posCol]);
    		}
    		catch (Exception e) {
    			throw new Exception (line);
    		}
			String otherEnd = this.pieces[infoCol];
			int idx1 = otherEnd.indexOf("END=");
			if (idx1 < 0) this.end = -1;
			else {
				int idx2 = otherEnd.indexOf(";", idx1+1);
				otherEnd = otherEnd.substring(idx1+4, idx2);
				this.end = Long.parseLong(otherEnd);
			}
	    	String [] fList = this.pieces[formatCol].split(":");
	    	String[] vList = this.pieces[sampleCol].split(":");
	    	int fNum = Math.min(fList.length, vList.length);
	    	for (int i=0; i<fNum; i++) {
	    		this.formatList.put(fList [i], vList[i]);
	    		this.formatFields.add(fList[i]);
	    	}
    	}
    	
    	public String genSampleField (List<String> formatList) {
    		StringBuffer buf = new StringBuffer();
    		for (int i=0; i<formatList.size(); i++) {
    			String f = formatList.get(i);
    			String fval = this.formatList.get(f);
    			if (fval==null) fval = "";
    			if (i>0) buf.append(":"); 
    			buf.append(fval);
    		}
    		return buf.toString();
    	}
    }
    
    private static int selectSample(List<String[]> sampleGvcfLineList, VcfLine[] sampleIdxList) throws Exception {
    	int sampleNum = sampleIdxList.length;
     	int leastSampleIdx = -1;
     	VcfLine leastVcfLine = null;
     	for (int i=0; i < sampleNum; i++) {
     		VcfLine vcfLine = sampleIdxList[i];
     		if (leastVcfLine == null) {
     			leastSampleIdx = i;
     			leastVcfLine = vcfLine;
     		}
     		else if (vcfLine != null) {
     			if (vcfLine.pos < leastVcfLine.pos) {
     				leastSampleIdx = i;
     				leastVcfLine = vcfLine;
     			}
     			else if (vcfLine.pos == leastVcfLine.pos) {
     				if (vcfLine.end < leastVcfLine.end) {
         				leastSampleIdx = i;
         				leastVcfLine = vcfLine;
     				}
     			}
     		}
     	}
     	
     	if (leastVcfLine == null) return -1;
     	else return leastSampleIdx;
    }
    
    /**
     * generate vcf line for all samples, keeping in mind that samples may come in in a different order than the sample ids received in the request and (will be) printed 
     * to the vcf file in the header.
     * Also, not all samples will have the same set of format fields, so will need to go through all samples to find all format fields and then
     * construct the format field values for each sample accordingly, preferably format fields are printed in the same order that the original samples used.
     * 
     * @param sampleGvcfLineList
     * @param sampleIdxList
     * @param curSampleIdx
     * @param sampleIdxNextList
     * @param vcfLineBuilder
     * @throws Exception
     */
    private void genVcfLine (List<String[]> sampleGvcfLineList, VcfLine[] sampleIdxList, int curSampleIdx, VcfLine[] sampleIdxNextList, StringBuilder vcfLineBuilder) throws Exception {

    	int sampleNum = vcfExtractSampleIdList.length;
    	VcfLine vcfLine0 = sampleIdxList[curSampleIdx];
    	if (vcfLine0.end > 0) {
    		// set all samples to be advanced by one row
    		for (int sIdx=0; sIdx < sampleIdxList.length; sIdx++) {
    			sampleIdxList[sIdx] = sampleIdxNextList[sIdx];
    			sampleIdxNextList [sIdx] = null;
    		}
    		return;
    	}
    	
    	// write generic fields
    	for (int i=0; i< formatCol; i++) {
    		vcfLineBuilder.append(vcfLine0.pieces[i]).append("\t");
    	}
    	
    	// write format field
    	List<String> formatList = new java.util.ArrayList<String>();
    	int fCount = 0;
    	for (VcfLine v: sampleIdxList) {
    		if (v==null) continue;
    		for (String key:v.formatFields) {
    			if (!formatList.contains(key)) {
    				if (key.equalsIgnoreCase("GT")) formatList.add(0, key);
    				else formatList.add(key);
    				if (fCount++ > 0) vcfLineBuilder.append(";");
    				vcfLineBuilder.append(key);
    			}
    		}
    	}
    	vcfLineBuilder.append("\t");
    	// write sample fields. samples may be listed in a different order than the VcfExtractSampleIdList
    	for (int i=0; i< sampleNum; i++) {
    		// find the sample record
    		VcfLine cVcfLine = null;
    		int theSampleIdx = -1;
    		if (i < sampleIdxList.length) {
    			cVcfLine = sampleIdxList[i];
    			theSampleIdx = i;
    		}
    		String curSampleID = vcfExtractSampleIdList[i];
    		if (cVcfLine==null || !cVcfLine.sampleId.equalsIgnoreCase(curSampleID)) {
    			for (int j=0; j< sampleIdxList.length; j++) {
    				VcfLine v = sampleIdxList[j];
    				if (v!=null && v.sampleId.equalsIgnoreCase(curSampleID)) {
    					cVcfLine = v;
    					theSampleIdx = j;
    					break;
    				}
    			}
    		}

    		if (cVcfLine == null) {
    			vcfLineBuilder.append(".\t");
    		}
    		else {
	    		if (i==curSampleIdx || cVcfLine.end < 0) {
	    			vcfLineBuilder.append(vcfLine0.genSampleField(formatList)).append("\t");
	    			sampleIdxList[theSampleIdx] = sampleIdxNextList[theSampleIdx];
	    			sampleIdxNextList[theSampleIdx] = null;
	    		}
	    		else {
	    			if (vcfLine0.pos >= cVcfLine.pos && vcfLine0.pos <= cVcfLine.end) {
	    				vcfLineBuilder.append(cVcfLine.genSampleField(formatList)).append("\t");
	    				if (vcfLine0.pos == cVcfLine.end) {
	    					sampleIdxList[theSampleIdx] = sampleIdxNextList[theSampleIdx];
	    					sampleIdxNextList[theSampleIdx] = null;
	    				}
	    			}
	    			else {
	    				vcfLineBuilder.append(".\t");
	    			}
	    		}
    		}
    	}
    	vcfLineBuilder.append("\n");
    }
    
    
    private static class GetGvcf implements Function<String, Get> {
    	private static final long serialVersionUID = 1L;
    	public Get call(String v) throws Exception {
    		return new Get(Bytes.toBytes(v));
    	}
    }
    
    private static class ResultGvcf implements Function<Result,String> {
    	private static final long serialVersionUID = 1L;
    	public String call(Result result) throws Exception {
    		String key = Bytes.toString(result.getRow());
    		List<KeyValue> kv = result.getColumn(gvcfCFBytes, gvcfColBytes);
    		if (kv==null || kv.isEmpty()) return null;
    		String ret = key + "##" + Bytes.toString(kv.get(0).getValue());
    		return ret;
    	}
    	
    }
  
    public static void main (String[] args) throws Exception {
    	boolean exit = false;
    	String cmd = null;
    	boolean runInCluster = false;
    	if (args.length < 2) {
    		exit = true;
    	}
    	else {
    		cmd = args[0];
    		runInCluster = args[1].equalsIgnoreCase("true");
    	}
    	if (args.length < 3 && (exit || cmd.equalsIgnoreCase("load"))) {
    		logger.info("Usage: java ... VcfLoadExtract load true/false vcfFiles");
    		exit = true;
    	}
    	else if (args.length < 4 && (exit || cmd.equalsIgnoreCase("extract"))) {
    		logger.info("Usage: java ... VcfLoadExtract extract true/false sampleIds outputFile deleteFileIfExists");
    		exit = true;
    	}
    	if (!exit) {
    		logger.info("initializing spark context...");
    		
    	    SparkConf sconf;
    	    JavaSparkContext sc;
    	    Configuration configuration;
    	    Connection hconnect;
    	    HBaseUtil hutil;

    		if (runInCluster) {
    			sconf = new SparkConf().setAppName("Spark-Hbase Connector");
    			sconf.set("spark.driver.host", "127.0.0.1");
    		}
    		else {
    			sconf = new SparkConf().setMaster("local").setAppName("Spark-Hbase Connector");
    			sconf.set("spark.driver.host", "127.0.0.1");
    		}
    		
    		
            sc = new JavaSparkContext(sconf);
            configuration = AutoConfigure.getConfiguration();
            hconnect = ConnectionFactory.createConnection(configuration);
            hutil = new HBaseUtil(hconnect);
//            hutil.dropTable(tableName);
            createTable(hutil);
            JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
    		
    		VcfLoadExtract vLE = new VcfLoadExtract();
    		try {
	 	    	if (cmd.equalsIgnoreCase("load")) {
		    		vLE.loadVcfFiles (hbaseContext, sc, args[2]);
		    	}
		    	else if (cmd.equalsIgnoreCase("extract")) {
		    		if (args.length >= 5 && args[4].equalsIgnoreCase("true")) {
		    			FileUtil.fullyDelete(new File(args[3]));
		    		}
		    		vLE.extractIntoVcfFile (sc, configuration, args[2], args[3]);
		    	}
		    	else throw new Exception ("Invalid cmd " + cmd + ", valid cmd: load, extract");
	    	}
    		finally {
    	        sc.close();
    	    	sc.stop();
    	        hconnect.close();
    	        if (!runInCluster) {
    		        AutoConfigure.stop();
    			}
    		}
    	}
    }
    
    private void loadVcfFiles (JavaHBaseContext hbaseContext, JavaSparkContext sc, String files) throws Exception{
    	String [] fileList = files.split(",");
    	int failedCount = 0;
    	logger.info("starting to load these gvcf/vcf files: " + files);
    	for (String file: fileList) {
    		try {
	    		String sampleID = file;
	    		int idx = sampleID.lastIndexOf(File.separator);
	    		if (idx >= 0) {
	    			sampleID = sampleID.substring(idx+1);
	    		}
	    		idx = sampleID.lastIndexOf(".");
	    		if (idx >= 0) {
	    			sampleID = sampleID.substring(0, idx);
	    		}
	    		logger.info("loading vcf file " + file + " with sampleID: " + sampleID);
	    		this.loadVcfFile(hbaseContext, sc, sampleID, file);
    		}
    		catch (Exception e) {
    			logger.error("failed laoding vcf file " + file, e);
    			failedCount++;
    		}
    	}
    	if (failedCount>0) {
    		throw new Exception ("Failed vcf load on " + failedCount + " out of total of " + fileList.length + " vcf files");
    	}
    }
    
    private void extractIntoVcfFile (JavaSparkContext sc, Configuration configuration, String sampleIds, String outFile) throws Exception {
    	String[] sampleIdList = sampleIds.split(",");
		String[] chrList = new String[] {"1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25"};
		logger.info("extracting samples into vcf file: " + outFile);
		try {
			this.extractVCF(sc, configuration, chrList, sampleIdList, outFile);
		}
		catch (Exception e) {
			logger.error("failed vcf extract for samples " + sampleIds, e);
			throw e;
		}
    }
    
    
    private static class SortVcf implements Function<String, Long> {
    	private static final long serialVersionUID = 1L;
    	private static Map<String, Long> chrLookup = new java.util.HashMap<String, Long>(25);
    	static {
    		chrLookup.put("1", 1L);
    		chrLookup.put("2", 2L);
    		chrLookup.put("3", 3L);
    		chrLookup.put("4", 4L);
    		chrLookup.put("5", 5L);
    		chrLookup.put("6", 6L);
    		chrLookup.put("7", 7L);
    		chrLookup.put("8", 8L);
    		chrLookup.put("9", 9L);
    		chrLookup.put("10", 10L);
    		chrLookup.put("11", 11L);
    		chrLookup.put("12", 12L);
    		chrLookup.put("13", 13L);
    		chrLookup.put("14", 14L);
    		chrLookup.put("15", 15L);
    		chrLookup.put("16", 16L);
    		chrLookup.put("17", 17L);
    		chrLookup.put("18", 18L);
    		chrLookup.put("19", 19L);
    		chrLookup.put("20", 20L);
    		chrLookup.put("21", 21L);
    		chrLookup.put("22", 22L);
    		chrLookup.put("23", 23L);
    		chrLookup.put("24", 24L);
    		chrLookup.put("25", 25L);
    		chrLookup.put("X", 23L);
    		chrLookup.put("Y", 24L);
    		chrLookup.put("MT", 25L);
    	}
    	public Long call(String key) throws Exception {
    		int idx = key.indexOf("\t");
    		key = key.substring(0, idx);
    		return chrLookup.get(key.toUpperCase());
    	}
    	
    }

}
