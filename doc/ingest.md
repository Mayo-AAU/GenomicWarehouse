# Ingest

Ingestion is the process of processing a `.gvcf` file to index information for later processing.

The goal here is to scale with the number of 'sequencers'.  The assumption is that single sample VCF files will be imported into the envoronment.   First, in a relatively raw form that can be accessed in Spark/Map-Reduce, but more importantly, also in a 'pre-joined' way inside HBase so that data can be rapidly extracted or cross queried for the clinical oncology use case.  (e.g. I have ~20 variants / genes, show me all samples that share the same variant as my current patient)

The scaling experement verifies that adding additional inputs from sequencers scales linearly with adding additional hardware on the cluster.

'Spark may be a great place to do the injest' -Dan B 2016
