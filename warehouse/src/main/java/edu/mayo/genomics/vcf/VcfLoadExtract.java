package edu.mayo.genomics.vcf;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
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
public class VcfLoadExtract {
    private static final String tableName = "GvcfTable";
    private static SparkConf sconf;
    private static JavaSparkContext sc;
    private static Configuration configuration;
    private static Connection hconnect;
    private static HBaseUtil hutil;
    private static String gvcfCol = "gvcf";
    private static String gvcfCF = "gvcfCF";
    private static byte[] gvcfColBytes = Bytes.toBytes(gvcfCol);
    private static byte[] gvcfCFBytes = Bytes.toBytes(gvcfCF);
	private static int formatCol = 8;
	private static int sampleCol = 9;
	private static int infoCol = 7;
	private static int posCol = 1;
	
	public static void init () throws Exception {
        sconf = new SparkConf().setMaster("local").setAppName("Spark-Hbase Connector");
        sconf.set("spark.driver.host", "127.0.0.1");
        sc = new JavaSparkContext(sconf);
        configuration = AutoConfigure.getConfiguration();
        hconnect = ConnectionFactory.createConnection(configuration);
        hutil = new HBaseUtil(hconnect);
        hutil.dropTable(tableName);
        hutil.createTable(tableName, new String[] { gvcfCF});
    }
	
	public static void stop () throws Exception {
        sc.close();
    	sc.stop();
        hconnect.close();
        AutoConfigure.stop();
	}
    
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

    public void loadVcfFile (String sampleID, String filePath)  throws Exception {
    	System.err.println ("*********************************");
    	JavaRDD<String> rdd = sc.textFile(filePath)
    			.filter(s -> !s.startsWith("#") )
    			.mapToPair(line -> new Tuple2<String, String> (getBucketID(line), line))
    			.reduceByKey((x,y) -> x+"\n" + y)
    			.map(x -> x._1 + "##" + sampleID + "##" + x._2);
         JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
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
    
    public void extractVCF (String [] chrList, String[] sampleIdList, String outFilePath) throws Exception {
        JavaRDD<String> chrListRdd = sc.parallelize(Arrays.asList(chrList));
        JavaRDD<String> sListRdd = sc.parallelize(Arrays.asList(sampleIdList));
        JavaRDD<String> rdd = chrListRdd.cartesian(sListRdd).map(x -> x._1 + "\t" + x._2);
        
        JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, configuration);
        
        // for now we have one row per sample per chr.
        JavaRDD<String> rdd2 = hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetFunction(), new ResultFunction())
        		.filter(x -> x!=null)
        		.mapToPair( x -> new Tuple2<String,String>(x.substring(0, x.indexOf("##")).split("\t")[0], x))
				.reduceByKey((x,y)-> x + "@@" + y)
				.map(x -> mergeGvcf(x._2.split("@@")));
        rdd2.saveAsTextFile(outFilePath);
        
//        List<String> resultList = rdd2.take(10);
//        for (int i=0; i< resultList.size(); i++) {
//        	if (resultList.get(i)!=null) {
//        		System.err.println(resultList.get(i));
//        	}
//        }
    }
    
    private static String mergeGvcf (String[] lineList) throws Exception {
    	StringBuilder retBuf = new StringBuilder();
    	int sampleNum = lineList.length;
    	VcfLine [] sampleIdxList = new VcfLine [sampleNum];
    	VcfLine [] sampleIdxNextList = new VcfLine [sampleNum];
    	
    	List<String[]> sampleGvcfLineList = new java.util.ArrayList<String []>(sampleNum);
    	for (int sIdx = 0; sIdx < lineList.length; sIdx++) {
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
    
    private static class VcfLine {
    	public int lineIdx;
    	public long pos;
    	public long end; // -1 indicates contains no end
    	public String rslt;
    	public String[] pieces;
    	public VcfLine (String line, int lineNum) throws Exception {
    		this.lineIdx = lineNum;
    		int idx0 = line.indexOf("##");
    		if (idx0 > 0) {
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
	    	this.rslt = this.pieces[sampleCol];
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
    
    private static void genVcfLine (List<String[]> sampleGvcfLineList, VcfLine[] sampleIdxList, int curSampleIdx, VcfLine[] sampleIdxNextList, StringBuilder vcfLineBuilder) throws Exception {

    	int sampleNum = sampleIdxList.length;
    	VcfLine vcfLine0 = sampleIdxList[curSampleIdx];
    	if (vcfLine0.end < 0) {
	    	for (int i=0; i<= formatCol; i++) {
	    		vcfLineBuilder.append(vcfLine0.pieces[i]).append("\t");
	    	}
	    	
	    	for (int i=0; i< sampleNum; i++) {
	    		VcfLine cVcfLine = sampleIdxList[i];
	
	    		if (cVcfLine == null) {
	    			vcfLineBuilder.append(".\t");
	    		}
	    		else {
		    		if (i==curSampleIdx || cVcfLine.end < 0) {
		    			vcfLineBuilder.append(vcfLine0.rslt).append("\t");
		    			sampleIdxList[i] = sampleIdxNextList[i];
		    			sampleIdxNextList[i] = null;
		    		}
		    		else {
		    			if (vcfLine0.pos >= cVcfLine.pos && vcfLine0.pos <= cVcfLine.end) {
		    				vcfLineBuilder.append(cVcfLine.rslt).append("\t");
		    				if (vcfLine0.pos == cVcfLine.end) {
		    					sampleIdxList[i] = sampleIdxNextList[i];
		    					sampleIdxNextList[i] = null;
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
    	else {
    		// set all samples to be advanced by one row
    		for (int sIdx=0; sIdx < sampleIdxList.length; sIdx++) {
    			sampleIdxList[sIdx] = sampleIdxNextList[sIdx];
    			sampleIdxNextList [sIdx] = null;
    		}
    	}
    }
    
    
    private static class GetFunction implements Function<String, Get> {
    	private static final long serialVersionUID = 1L;
    	public Get call(String v) throws Exception {
    		return new Get(Bytes.toBytes(v));
    	}
    }
    
    private static class ResultFunction implements Function<Result,String> {
    	private static final long serialVersionUID = 1L;
    	public String call(Result result) throws Exception {
    		String key = Bytes.toString(result.getRow());
    		List<KeyValue> kv = result.getColumn(gvcfCFBytes, gvcfColBytes);
    		if (kv==null || kv.isEmpty()) return null;
    		String ret = key + "##" + Bytes.toString(kv.get(0).getValue());
    		return ret;
    	}
    	
    }
  
}
