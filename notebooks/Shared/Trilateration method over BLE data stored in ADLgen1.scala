// Databricks notebook source
//variables
//sc
//sqlContext
//spark
//spark.version
//dbutils.notebook.getContext.tags("sparkVersion")
spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

// COMMAND ----------

// MAGIC %md There are two options to read and write Azure Data Lake data from Azure Databricks:
// MAGIC 1. DBFS mount points
// MAGIC 2. Spark configs

// COMMAND ----------

// MAGIC %md ## 1 - DBFS mount points
// MAGIC [DBFS](https://docs.azuredatabricks.net/user-guide/dbfs-databricks-file-system.html) mount points let you mount Azure Data Lake Store for all users in the workspace. Once it is mounted, the data can be accessed directly via a DBFS path from all clusters, without the need for providing credentials every time. The example below shows how to set up a mount point for Azure Data Lake Store.

// COMMAND ----------

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> "xxxxxxxxxxxxxxxxxxxx",
  "dfs.adls.oauth2.credential" -> "xxxxxxxxxxxxxxxxxxxxxxxx",
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/xxxxxxxxxxxxxxxxxxxxxxx/oauth2/token")

dbutils.fs.mount(
  source = "adl://muhady.azuredatalakestore.net/",
  mountPoint = "/mnt/kp",
  extraConfigs = configs)

// COMMAND ----------

//unmount DBFS
dbutils.fs.unmount("/mnt/kp")

// COMMAND ----------

//just sample
//%fs ls /mnt/test

// COMMAND ----------

//read avro files stored in data lake storagevar 
var avroDf = spark.read.format("com.databricks.spark.avro").load("/mnt/kp/test/muhady/rapsberrytest/0/2019/07/21/*/*/*.avro")


// COMMAND ----------

//avroDf.printSchema()


// COMMAND ----------

//avroDf.select("Offset","EnqueuedTimeUtc","Body").write.saveAsTable("tab2")

// COMMAND ----------

val row=avroDf.select("Offset","EnqueuedTimeUtc","Body").createOrReplaceTempView("tabulka")

// COMMAND ----------

//spark.sql("""select * from tabulka""")

// COMMAND ----------

//spark.sql("""select cast(Body as string) from tabulka""").show()

// COMMAND ----------

spark.sql("""select 
                Offset,
                EnqueuedTimeUtc, 
                     substring_index(cast(Body as string),';',1) as Timestamp_epoch,
                     FROM_UNIXTIME(substring_index(cast(Body as string),';',1)) as Timestamp,
                     substring_index(substring_index(cast(Body as string),';',2), ';',-1) as Locator,
                     substring_index(substring_index(cast(Body as string),';',3), ';',-1) as MAC,
                cast(substring_index(cast(Body as string),';',-1) as int) as RSSI 
             
             from tabulka""").createOrReplaceTempView("tabulka2")

// COMMAND ----------

//spark.sql("""select * from tabulka2""").show()

// COMMAND ----------

//display(spark.sql("""select  
//                *
//             from tabulka2 where Timestamp_epoch between 1563733620 and 1563733650 order by RSSI """))

// COMMAND ----------

//spark.sql("""select  
//                Locator, 
//                Avg(RSSI) as AVG_RSSI, 
//                percentile_approx(RSSI, 0.5) as DEV_RSSI 
//             from tabulka2 where Timestamp_epoch between 1563734240 and 1563734270 group by Locator""").show()

// COMMAND ----------

//spark.sql("""SELECT unix_timestamp(now())""").show()

// COMMAND ----------

spark.sql("""select 
                round(Avg(RSSI),2) as AVG_RSSI, 
                round(percentile_approx(RSSI, 0.5),2) as DEV_RSSI,
                count(RSSI) as count, 
                Locator, 
                TimeStamp,
                Timestamp_epoch,
                MAC 
              from tabulka2 
              group by Timestamp,Timestamp_epoch, Locator,MAC order by TimeStamp desc""").createOrReplaceTempView("tabulka3")

// COMMAND ----------

//spark.sql("""select * from tabulka3 where Timestamp_epoch>=1563725000""").show()

// COMMAND ----------

spark.sql("""SELECT * FROM (
   SELECT AVG_RSSI, TimeStamp,Timestamp_epoch,MAC, Locator FROM tabulka3
 ) PIVOT (
   avg(AVG_RSSI)
   FOR Locator IN ('raspberrypi1','raspberrypi2','raspberrypi3','raspberrypi4'))
 """).createOrReplaceTempView("tabulka4")

spark.sql("""select * from tabulka4""").show()

// COMMAND ----------

spark.sql("""select TimeStamp, TimeStamp_epoch, MAC,
              round(power(10,(((-1.0*raspberrypi1)-61.2)/(10*1.57))),2) as rasp1m,
              round(power(10,(((-1.0*raspberrypi2)-61.2)/(10*1.57))),2) as rasp2m,
              round(power(10,(((-1.0*raspberrypi3)-61.2)/(10*1.57))),2) as rasp3m,
              round(power(10,(((-1.0*raspberrypi4)-61.2)/(10*1.57))),2) as rasp4m
            from tabulka4 where Timestamp_epoch>1563738960 order by Timestamp_epoch desc""").createOrReplaceTempView("tabulka5")

// COMMAND ----------

spark.sql("""select * from tabulka5""").show()

// COMMAND ----------

val dfinal=spark.sql("""select * from tabulka5""").toDF()



// COMMAND ----------

dfinal.show()

// COMMAND ----------

import breeze.linalg.{DenseMatrix, inv}
def trilat(d1:Double, d2:Double, d3:Double, d4:Double) : DenseMatrix[Double] = {
    val x1:Double = 0.0
    val y1:Double = 0.0
    val x2:Double = 3.33
    val y2:Double = 0.0
    val x3:Double = 3.33
    val y3:Double = 3.33
    val x4:Double = 0.0
    val y4:Double = 3.33

    if ( d1.isNaN || d2.isNaN || d3.isNaN || d4.isNaN)
      return DenseMatrix(0.0,0.0)
    else {
      val A = DenseMatrix((2 * (x1 - x4), 2 * (y1 - y4)), (2 * (x2 - x4), 2 * (y2 - y4)), (2 * (x3 - x4), 2 * (y3 - y4)))
      val b = DenseMatrix(((x1 * x1) - (x4 * x4) + (y1 * y1) - (y4 * y4) + (d4 * d4) - (d1 * d1)), ((x2 * x2) - (x4 * x4) + (y2 * y2) - (y4 * y4) + (d4 * d4) - (d2 * d2)),((x3 * x3) - (x4 * x4) + (y3 * y3) - (y4 * y4) + (d4 * d4) - (d3 * d3)))

      val At= A.t
      val Atm= At * A
      val Atminv= inv(Atm)

      return (Atminv * (At * b))
    }
}

case class EntryItem(TimeStamp:String, TimeStamp_epoch:String, MAC:String, d1:Double,d2:Double,d3:Double,d4:Double,x:Double, y:Double)



//dfinal.select(trilat($"rasp1m",$"rasp2m",$"rasp3m",$"rasp4m").valueAt(0,0), $"rasp1m").show()

//val B=DenseMatrix((2.0,7.0),(3.0,-5.0))
//val transpose=simpleMatrix.t
     
//val inverse=inv(simpleMatrix)

     
//simpleMatrix * inverse

// COMMAND ----------

import  java.lang.Double

var d1:Double=0.0
var d2:Double=0.0
var d3:Double=0.0
var d4:Double=0.0
var TimeStamp:String=""
var TimeStamp_epoch:String=""
var MAC:String=""
var tab:Seq[EntryItem]=Seq(EntryItem("a","a","a",1.0,1.0,1.0,1.0,1.0,1.0))

for(row <- dfinal.filter($"rasp1m".isNotNull && $"rasp2m".isNotNull && $"rasp3m".isNotNull && $"rasp4m".isNotNull).rdd.collect)
{ 
  TimeStamp=row(0).toString
  TimeStamp_epoch=row(1).toString
  MAC=row(2).toString
  
  if(row(3) != null)
    d1 = Double.parseDouble(row(3).toString)
  else 
    d1 = 0.0;
  
  if(row(4) != null)
    d2 = Double.parseDouble(row(4).toString)
  else 
    d2 = 0.0;
  
  if(row(5) != null)
    d3 = Double.parseDouble(row(5).toString)
  else 
    d3 = 0.0;

  if(row(6) != null)
    d4 = Double.parseDouble(row(6).toString)
  else 
    d4 = 0.0;
  
  var res = trilat(d1,d2,d3,d4)
  //println (res.valueAt(0,0) + " " + res.valueAt(1,0))

  var a = EntryItem(TimeStamp,TimeStamp_epoch,MAC,d1,d2,d3,d4,res.valueAt(0,0),res.valueAt(1,0))
  tab = tab :+ a 
  
}

display(tab.toDF())


// COMMAND ----------

// MAGIC %md ##2 - Spark Configs
// MAGIC 
// MAGIC With Spark configs, the Azure Data Lake Store settings can be specified per notebook. To keep things simple, the example below includes the credentials in plaintext. However, we strongly discourage you from storing secrets in plaintext. Instead, we recommend storing the credentials as [Databricks Secrets](https://docs.azuredatabricks.net/user-guide/secrets/index.html#secrets-user-guide).
// MAGIC 
// MAGIC **Note:** `spark.conf` values are visible only to the DataSet and DataFrames API. If you need access to them from an RDD, refer to the [documentation](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html#access-azure-data-lake-store-using-the-rdd-api).

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "xxxxxxxxxxxxxxxxxxxxxxx")
spark.conf.set("dfs.adls.oauth2.credential", "xxxxxxxxxxxxxxxxxxxxxxxx")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/xxxxxxxxxxxxxxxxxxxxxxx/oauth2/token")

// COMMAND ----------

// MAGIC %fs ls adl://muhady.azuredatalakestore.net/test/

// COMMAND ----------

spark.read.parquet("dbfs:/mnt/my-datasets/datasets/iot/events").write.mode("overwrite").parquet("adl://kpadls.azuredatalakestore.net/testing/tmp/kp/v1")

// COMMAND ----------

// MAGIC %fs ls adl://kpadls.azuredatalakestore.net/testing/tmp/kp/v1