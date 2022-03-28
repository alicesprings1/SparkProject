package bigdata.hw3.q3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object GunStat {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("GunStat")
      .getOrCreate()
    val df=spark
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv("hw3/data/crime_2010_2018")
      .withColumn("YEAR",regexp_extract(col("REPORT_DAT"),"\\d+",0))
    df.select("YEAR","METHOD")
      .na.drop()
    df.createOrReplaceTempView("crime_2010_2018")
    spark.sql(
      """
        |select t1.YEAR,METHOD,NUM/TOTAL_NUM as RATIO from
        |(
        |select YEAR,METHOD,count(METHOD) as NUM
        |from crime_2010_2018
        |group by YEAR,METHOD)t1
        |join (
        |select YEAR,count(*) as TOTAL_NUM
        |from crime_2010_2018
        |group by YEAR
        |)t2 on t1.YEAR=t2.YEAR and METHOD="GUN"
        |order by t1.YEAR
        |""".stripMargin)
      .show()
    spark.stop()
  }
}
