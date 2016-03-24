package edu.mayo.genomics.model;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

/**
 * Class for Storing Sample Metadata
 */
public class Sample {
    private String sampleID;             // User readable identifier for the sample 'MySample',
    HashMap<String,String> metadata;   // an arbitrary hash of metadata for the sample

    /**
     * This constructor is used when you want to pull data from something like hbase
     * @param sampleID
     */
    public Sample(String sampleID){
        this.sampleID = sampleID;
    }

    /**
     *
     * This constructor is used when you want to populate the object from a file
     * @param sampleID           - this is an ID controled by the LIMS system so that we can link back to the sample in the freezer
     * @param pathToMetadataFile - this is a simple properties file with key-value pairs for all metadata attributes we wish to store in the system
     */
    public Sample(String sampleID, String pathToMetadataFile) throws IOException {
        this.sampleID = sampleID;
        parseMetadata(pathToMetadataFile);
    }

    public HashMap<String,String> parseMetadata(String pathToMetadataFile) throws IOException {
        metadata = new HashMap<String,String>();
        BufferedReader br = new BufferedReader(new FileReader(pathToMetadataFile));
        String line;
        while((line = br.readLine())!= null){
            String[] tokens = line.split("=");
            if(tokens.length == 2){
                metadata.put(tokens[0].trim(), tokens[1].trim());
            }
        }
        return metadata;
    }

    public String getSampleID() {
        return sampleID;
    }

    public void setSampleID(String sample) {
        this.sampleID = sample;
    }

    public HashMap<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(HashMap<String, String> metadata) {
        this.metadata = metadata;
    }
}
