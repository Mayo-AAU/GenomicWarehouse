# GenomicWarehouse
This is an open source project that attempts to answer important genomics questions on the big data platform

Make sure you develop using Java SDK 1.8+

## High Level Goals

1. [Recalculate](doc/recalculate.md)
2. [Ingest](doc/ingest.md)
3. [Export](doc/export.md)
4. [Filter](doc/filter.md)
5. [Interaction](doc/interaction.md)

## Plan

- [ ] Create infrastructure for evaluation
	1. [local sandbox](doc/setup.md) for testing / development
	2. "small cluster", 4x 8C/32G/10T+
	3. "medium cluster", 8x 8C/32G/10T+
	3. "large cluster", 32x 8C/32G/10T+
- [ ] Write [ingest code](doc/ingest.md)
   1. create schema in HBase
   2. ingest sample `.gvcf` files
- [ ] Write [filter code](doc/filter.md)
   1. identity filter
- [ ] Load sample datasets
- [ ] Write [export code](doc/export.md)
- [ ] Write [recalculate code](doc/recalculate.md)
- [ ] Write final report

## Big Hairy Audacious Goals (BHAGs)

This project is a "jugular experiment".  It is designed to evaluate if the [Hadoop](https://hadoop.apache.org/) big data platform is suitable for analytics on genomic data.

- copy `VCF` files to HDFS
- index `VCF` from HDFS into HBase
- perform standard queries againist `VCF` data in HBase

