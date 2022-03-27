package bigdata.hw3.q3

import org.apache.spark.sql.SparkSession

object DataClean {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("DataClean")
      .getOrCreate()
    val df=spark
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv("hw3/data/Crime_Incidents_in_2013.csv")
    df.select("CCN","REPORT_DATE","OFFENSE","METHOD","END_DATE","DISTRICT")
      .na.drop()
      .repartition(1)
      .write
      .option("header","true")
      .csv("hw3/output/q3a")

    spark.stop()
  }
}
