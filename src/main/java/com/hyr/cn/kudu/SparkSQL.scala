package com.hyr.cn.kudu

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
 *
 * @date 2021-03-01 4:51 下午
 * @author: <a href=mailto:huangyr@>huangyr</a>
 * @Description: spark查询Kudu
 ******************************************************************************/
object SparkSQL {

  def main(args: Array[String]): Unit = {
    val kuduMaster = "server1:7051,server2:7051,server3:7051"
    val spark = SparkSession.builder
      .appName(this.getClass.getName)
      .master("local[8]") // cores设置为和表分片数一致
      .getOrCreate
    // Read a table from Kudu
    val dataframe = spark.read
      .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> "user"))
      .format("kudu").load

    // Query using the Spark API...
    dataframe.select("*").filter("id >= 5").show(numRows = 20, truncate = false)

    // 创建kuducontext
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    // 根据dataframe的schema新建一张表test_table
    val test_table = "test_table"
    if (kuduContext.tableExists(test_table)) {
      kuduContext.deleteTable(test_table)
    }
    import collection.JavaConverters._
    kuduContext.createTable(
      test_table, dataframe.schema, Seq("id"),
      new CreateTableOptions()
        .setNumReplicas(3)
        .addHashPartitions(List("id").asJava, 3))

    // Check for the existence of a Kudu table
    kuduContext.tableExists(test_table)

    // Insert data  dataframe的每个分片(kudu Table的每个Tablet)都会生成一个task
    kuduContext.insertRows(dataframe, test_table)

    // Delete data
    kuduContext.deleteRows(dataframe, test_table)

    // Upsert data
    kuduContext.upsertRows(dataframe, test_table)

    // Delete data
    // Update data
    kuduContext.updateRows(dataframe, test_table)

    // Data can also be inserted into the Kudu table using the data source, though the methods on
    // KuduContext are preferred
    // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert
    // in the options map
    // NB: Only mode Append is supported
    // 写入Kudu
    dataframe.write
      .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> test_table))
      .mode("append")
      .format("kudu").save

    // http://localhost:4040
    Thread.sleep(10000000)
    spark.close()
  }

}
