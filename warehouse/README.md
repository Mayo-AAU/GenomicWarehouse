# GenomicWarehouse

This is an open source project that attempts to answer important genomics questions on the big data platform

Make sure you develop using Java SDK 1.8+

## High Level Goals

1. [Recalculate](doc/recalculate.md)
2. [Ingest](doc/ingest.md)
3. [Export](doc/export.md)
4. [Filter](doc/filter.md)
5. [Interaction](doc/interaction.md)

## Setup requirements

- Essentials
  - [ ] **DJB** Be able to scale cluster up and down as needed
  - [ ] **DJB** [Hortonworks Data Platform](http://hortonworks.com/hdp/) `v2.3` installed on Azure
  - [ ] Install Java v8
  - [ ] HDFS
  - [ ] Spark
  - [ ] HBase
  - [ ] Create infrastructure for evaluation
	 1. [local sandbox](doc/setup.md) for testing / development
	 2. "small cluster", 3x 8C/32G/10T+
	 3. "medium cluster", 5x 8C/32G/10T+
	 4. "large cluster", 7x 8C/32G/10T+
- Optional, but nice to have
  - [ ] Jypter
  - [ ] Zepplin

## Plan

- [ ] **DJQ** Test/Dev environment https://github.com/drachimera/HadoopCommons
- [ ] Write [ingest code](doc/ingest.md)
   1. create schema in HBase
   2. ingest sample `.gvcf` files
- [ ] Write [filter code](doc/filter.md)
   1. identity filter
- [ ] Load sample datasets
- [ ] Write [export code](doc/export.md)
- [ ] Write [recalculate code](doc/recalculate.md)
- [ ] **DJQ** Write final report *Google Doc*



