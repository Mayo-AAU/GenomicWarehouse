package edu.mayo.genomics.model;

import java.io.Serializable;

/**
 * Created by m102417 on 12/23/15.
 *
 * Kit is a concept that shows where on the genome a given assay has coverage.  e.g. in whole genome sequencing, all chroms all positions are covered
 * but in whole exmome sequencing, it is just the genes that are covered.
 *
 * Kit will usually be read and then broadcast to all of the worker nodes because it is small (several thousand lines at the most)
 * So there is no need to embed Kit into any of the variant data.
 *
 */
public class Kit implements Serializable {
    int kID;         //the kit's ID
    String kitName;  //e.g. 'custom capture'
    String path;   //the (hdfs) location of the bed file for the kit
}
