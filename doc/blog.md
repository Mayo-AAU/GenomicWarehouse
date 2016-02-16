# 2016-02-15 - DJB

Trying to install Cloudbreak via the [Azure "one-click" installer](http://sequenceiq.com/cloudbreak-docs/latest/azure/).  However, Azure puked with an error about passwords (why they let it be entered incorrectly?!?):

```
"message":"The supplied password must be between 6-72 characters long and must satisfy at least 3 of password complexity requirements from the following: \r\n1) Contains an uppercase character\r\n2) Contains a lowercase character\r\n3) Contains a numeric digit\r\n4) Contains a special character"
```

Trying again.

OK, came up, but nothing was installed.  Logging in with username `cloudbreak`, and running the [installer from SequenceIQ](https://raw.githubusercontent.com/sequenceiq/azure-cbd-quickstart/master/install-cbd.sh).  Oops, had to start docker first.  Rockin' now.

# 2016-02-12 - DJB

Azure performance isn't all that great.  Putting a `15G` `.gvcf` file into HDFS has been taking over 30 minutes(!).  Not sure what is happening...

```bash
sandbox@hn0-bd4gsa:~/vcf$ ls -lah *.gvcf
-rwxrwxr-x 1 sandbox sandbox 15G Feb 10 16:10 NA_1401601612.gvcf

# Go get some coffee for this one...
sandbox@hn0-bd4gsa:~/vcf$ hadoop fs -put NA_1401601612.gvcf vcf/NA_1401601612.gvcf
```

## Spark results

Running `vcf_test.scala` 'interactively' within the `spark-shell` to test how fast we can read `.gvcf` data.  The basics are:

```scala
val vcfHDFS = sc.textFile("vcf/NA_1401601612.gvcf")
// ...
// Count entries by chromosome, show the first 5
val byCount = byChromosome.reduceByKey ( _ + _ )
byCount.sortBy ( _._2, false).take(5)
```

For a `.gz` file, processing took 613 seconds, for a `.bz2` [bzip2](http://www.bzip.org/) file, 535 seconds, for an uncompressed `.gvcf` Spark required 140 seconds.  

## Using AWK

This `awk` script does essentially the same thing as the fancy Spark, but one thread only.


```bash
time bunzip2 -c NA_1401601612.gvcf.bz2 |  awk '!/#/ {
 Ch[$1]++;
 }
 END{
 for (var in Ch)
 print "Chrom:", var, "has", Ch[var],"entries"
 }
 '
```

It took `535 seconds` to run (`user	8m55.572s`).  Hmm, suspiciously, this looks like the same amount of time for the Spark version. I'm guessing we're dominated by disk and/or decompression.

<<<<<<< HEAD
If instead of letting Spark determine the number of partitions, [let's choose the number](http://www.bigsynapse.com/spark-input-output).
=======
If instead of letting Spark determine the number of partitions, let's choose the number.
>>>>>>> Thoughts on Spark performance

```scala
val vcfFilename = "vcf/NA_1401601612.gvcf.bz2"
val vcfHDFS = sc.textFile(vcfFilename, 20)
```

Low and behold!  The time was `174s`, 5x faster than the default partitions set by `sc.defaultMinPartitions`.

Trying the same thing for a `.gz` still gives 1 partition (not 20 as expected), so Spark can not partition a `.gz` file.  Bummer, because all the `.vcf` files are typically compressed.

## Uncompressed performance

Currently loading an uncompressed file to check Spark performance.  Wondering if the data can be processed concurrently?

Here's an interesting tidbit!  Spark reports the number of partitions for the different files:

| Type | Size | #partitions  | partition size  | notes   |
|-----|------|---|---|---|
| raw    | 15G     | 29  | 512M  | Will process concurrently on 29 jobs?  |
|  gzip   |  1.5G    | 1  | 1.5G  | Spark can not break this file up into partitions?  Will be linear processing.  |
|  bzip2   |  1G    | 2  | 512M  | Each partition contains a lot of data.  |