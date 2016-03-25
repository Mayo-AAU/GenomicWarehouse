# Experiments using VCF on HDP

Copy data from RCF cluster, using `rcfcluster-ftp.mayo.edu`:

```
cd /data2/TRC/prod/done/old
rsync -v 12*.gz bd4g:
rsync -v NA_14*.gz bd4g:
rsync -v 1*.gz bd4g:
```

## Moving to HDFS

```
hadoop fs -mkdir /tmp/vcf
hadoop fs -put NA_1424005550.gvcf.gz /tmp/vcf/
[sandbox@bd4gsandbox-master-01 VCF]$ hdfs dfs -ls /tmp/vcf
Found 1 items
-rw-r--r--   3 sandbox hdfs   40256771 2016-02-05 17:18 /tmp/vcf/NA_1424005550.gvcf.gz
```

## Java 8 on HDP

```
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u65-b17/jdk-8u65-linux-x64.tar.gz
```


## Setup Spark?