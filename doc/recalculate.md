# Recalculate

Recalculation is the process of updating a `VCF` based on the arrival of new data.

Issues:
- Code complexity, RDD/DataFrame.  How much code do we have to wright and how difficult is it to understand and maintain?  How would this compare to mongoDB's aggregation framework?  How hard would it be to support?
- Runtime experement (below)

Runtime Experement Variables of Freedom:
- variants
- samples
- cluster size
