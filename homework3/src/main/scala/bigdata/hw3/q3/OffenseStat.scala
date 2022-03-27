package bigdata.hw3.q3

import org.apache.spark.sql.SparkSession

object OffenseStat {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("OffenseStat")
      .getOrCreate()
    val df=spark
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv("hw3/data/Crime_Incidents_in_2013.csv")
    df.createOrReplaceTempView("crime_2013")
    spark.sql(
      """
        |select OFFENSE, count(*) as COUNT
        |from crime_2013
        |group by OFFENSE
        |""".stripMargin).show()
    spark.stop()
  }
}
