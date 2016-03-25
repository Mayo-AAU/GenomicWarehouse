# Ingest

Ingestion is the process of processing a `.gvcf` file to index information for later processing.

The goal here is to scale with the number of 'sequencers'.  The assumption is that single sample VCF files will be imported into the environment.   First, in a relatively raw form that can be accessed in Spark/Map-Reduce, but more importantly, also in a 'pre-joined' way inside HBase so that data can be rapidly extracted or cross queried for the clinical oncology use case.  (e.g. I have ~20 variants / genes, show me all samples that share the same variant as my current patient)

The scaling experiment verifies that adding additional inputs from sequencers scales linearly with adding additional hardware on the cluster.

> 'Spark may be a great place to do the ingest' -Dan B 2016

# Experimental design

Develop code to do the following:

1. Read VCF data from HDFS
2. Populate Hadoop systems (HBase, HDFS, ORC, etc) to facilitate [filtering](filter.md), [recalculation](recalculate.md), and [export](export.md)

Test the following:

1. Scale with many concurrent ingestion processing running
2. Scale with large amounts of data previously ingested
