# GenomicWarehouse
This is an open source project that attempts to answer important genomics questions on the big data platform

Make sure you develop using Java SDK 1.8+

## High Level Goals

1. [Recalculate](doc/recalculate.md)
2. [Injest](doc/ingest.md)
3. [Export](doc/export.md)
4. [Filter](doc/filter.md)
5. [Interaction](doc/interaction.md)

## Big Hairy Audacious Goals (BHAGs)

This project is a "jugular experiment".  It is designed to evaluate if the [Hadoop](https://hadoop.apache.org/) big data platform is suitable for analytics on genomic data.

- copy `VCF` files to HDFS
- index `VCF` from HDFS into HBase
- perform standard queries againist `VCF` data in HBase

