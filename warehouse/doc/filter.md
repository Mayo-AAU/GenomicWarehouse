# Filter

Include only certain records in an in-memory virtual `.vcf`.

A Filter needs to be done in near real time, just like VCF-Miner.  Here is a relevant query that touches both INFO (Annotation) and sample information.

Degrees of freedom:
- Number of machines in the cluster
- Number of variants
- Number of samples


1. run the query to get a baseline value.
2. load more data
3. run the query again to get another value, continue until plot is finished
4. scale the cluster and repeat.
