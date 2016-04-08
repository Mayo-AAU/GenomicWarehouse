package edu.mayo.hadoop.commons.examples;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class VcfFormatFields implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String,String> fList;
	
	public VcfFormatFields (String formatField, String sampleField) {
		String[] sList = sampleField.split(";");
    	String[] formatList = formatField.split(";");
		this.fList = new java.util.HashMap<String,String>(sList.length);
		
		for (int i=0; i< sList.length; i++) {
    		this.fList.put(formatList[i], sList[i]);
    	}
	}
	
	public byte[] serialize () throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
		  out = new ObjectOutputStream(bos);   
		  out.writeObject(this);
		  byte[] retBytes = bos.toByteArray();
		  return retBytes;
		} 
		finally {
			try {
			    if (out != null) {
			      out.close();
			    }
			} 
			catch (IOException ex) {
			    // ignore close exception
			}
			try {
			    bos.close();
			} 
			catch (IOException ex) {
			    // ignore close exception
			}
		}
	}
	
	public static VcfFormatFields deserialize (byte[] bytes) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null; 
		try {
			  in = new ObjectInputStream(bis);
			  @SuppressWarnings("unchecked")
			  VcfFormatFields o = (VcfFormatFields) in.readObject();
			  return o;
		} 
		finally {
			  try {
			    bis.close();
			  } 
			  catch (IOException ex) {
			    // ignore close exception
			  }
			  try {
			    if (in != null) {
			      in.close();
			    }
			  } 
			  catch (IOException ex) {
			    // ignore close exception
			  }
		}
	}
	
	public String toString () {
		StringBuffer formatBuf = new StringBuffer();
		StringBuffer sampleBuf = new StringBuffer();
		int i = 0;
		for (java.util.Map.Entry<String,String> entry: this.fList.entrySet()) {
			if (i++ > 0) {
				formatBuf.append(",");
				sampleBuf.append(",");
			}
			formatBuf.append(entry.getKey());
			sampleBuf.append(entry.getValue());
		}
		return formatBuf.append("\t").append(sampleBuf).toString();
		
	}
	
	public static String toVCF (Result result) throws IOException, ClassNotFoundException {
		if (result.listCells()==null) return null;
		String chr = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.chrBytes).get(0).getValue());
		String pos = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.posBytes).get(0).getValue());
		String id = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.idBytes).get(0).getValue());
		String ref = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.refBytes).get(0).getValue());
		String alt = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.altBytes).get(0).getValue());
		String qual = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.qualBytes).get(0).getValue());
		String filter = Bytes.toString(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.filterBytes).get(0).getValue());
		String formatSample = VcfFormatFields.deserialize(result.getColumn(SparkHBaseITCase.cf1Bytes, SparkHBaseITCase.sampleBytes).get(0).getValue()).toString();
		StringBuffer buf = new StringBuffer();
		buf.append(chr).append("\t").append(pos).append("\t").append(id).append("\t").append(ref).append("\t").append(alt).append("\t").append(qual).append("\t").append(filter).append("\t").append(formatSample);
		
		return buf.toString();
	}
}
