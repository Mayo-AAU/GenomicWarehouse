// Remove all the info junk...
sc.setLogLevel("WARN")


<<<<<<< Updated upstream
val vcfHDFS = sc.textFile("vcf/NA_1401601612.gvcf")
=======
val vcfHDFS = sc.textFile(vcfFilename, numPartitions)

print ( "Found " + vcfHDFS.partitions.length + " partitions for " + vcfFilename)
>>>>>>> Stashed changes

// each record in vcfHDFS is an array of the words
// get rid of spaces, and map to lower case

val noHeader = vcfHDFS.filter( _(0) != '#')

// Split by white space
val vcf = noHeader.map( _.split("\\s+"))

val byChromosome = vcf.map ( item => ( item(0), 1 ))

// Collect by gene (1st entry)
val byCount = byChromosome.reduceByKey ( _ + _ )

val startTime = System.currentTimeMillis()
byCount.sortBy ( _._2, false).take(5)
val endTime = System.currentTimeMillis()

val difference = endTime - startTime

print ( "difference is " + difference/1000)

// GZip'd version took 613 seconds vcf/NA_1401601612.gvcf.gz
// BZ2 version took difference is 535

/*
The awk version...

bzcat 1KG.chr22.anno.infocol.vcf.bz2 |  awk '!/#/ {
 Ch[$1]++;
 }
 END{
 for (var in Ch)
 print "Chrom:", var, "has", Ch[var],"entries"
 }
 ' 

*
*
* Running both gives:
* Scala:

scala> byCount.sortBy ( _._2, false).take(5)
res14: Array[(String, Int)] = Array((22,348110))

*
* AWK:
* Chrom: 22 has 348110 entries
*/
