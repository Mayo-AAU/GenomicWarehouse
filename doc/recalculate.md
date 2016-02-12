# Recalculate

Recalculation is the process of updating a `VCF` based on the arrival of new data.

## Issues

- Code complexity, RDD/DataFrame.  How much code do we have to write and how difficult is it to understand and maintain?  How would this compare to mongoDB's aggregation framework?  How hard would it be to support?
- Runtime experiment (below)

Runtime Experiment Variables of Freedom:
- variants
- samples
- cluster size

## Experiment

Assume [ingest](ingest) is working properly.  Continue adding `VCF` files.  After each ingestion, run a series of recalculations and measure wall time vs. number of nodes.