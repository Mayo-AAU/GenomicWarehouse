package edu.mayo.genomics.model;

import java.util.HashMap;

/**
 * Created by m102417 on 2/4/16.
 */
public class FormatData {
    String sampleID; //the sample identifier for this FormatData
    HashMap<String,String> formatData;  //all of the data for a given sample stored in a map.
    //examples of formatdata:
    //GT : "./1",
    //PL : "0,24,1045",
    //AD_1 : 23,
    //AD_2 : 44,
    //GQ : 40,
    //HQ : "24,40",
    //DP : 20,
    //MIN_DP : 20,
    //gene : "BRCA2",
    //sgV : 1 #sanger validated,
    //GTC : 1
}
