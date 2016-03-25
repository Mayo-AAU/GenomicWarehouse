package edu.mayo.hadoop.commons.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import edu.mayo.hadoop.commons.hbase.AutoConfigure
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import edu.mayo.hadoop.commons.minicluster.MiniClusterUtil

object HBaseTest {
  def main(args: Array[String]) {
    println("Hello, world\n\n\n\n\n\n")
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tableName = TableName.valueOf("spark-test")

    val configuration = AutoConfigure.getConfiguration()
    configuration.set(TableInputFormat.INPUT_TABLE, tableName.getNameAsString())
    val connection = ConnectionFactory.createConnection(configuration)

    HBaseAdmin.checkHBaseAvailable(configuration)
    val admin = connection.getAdmin()

    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("column-family"))
      admin.createTable(tableDesc)

      // Add 100 rows
      val connection = ConnectionFactory.createConnection(configuration)
      val table = connection.getTable(tableName)
      for (i <- 1 to 100) {
        val put = new Put(Bytes.toBytes(i))
        put.addColumn(Bytes.toBytes("column-family"), Bytes.toBytes("column"), Bytes.toBytes(i))
        table.put(put)
      }

    }
    val hBaseRDD = sc.newAPIHadoopRDD(configuration, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println("\n\n\n\n\nFound " + hBaseRDD.count() + " rows \n\n\n\n\n\n")

    sc.stop()

    admin.close()

    println("\n\n\n\n\n\nAll ur HBase are belong to us!!!\n\n\n\n\n\n")

    MiniClusterUtil.stopAll();
    System.exit(0)
  }
}
