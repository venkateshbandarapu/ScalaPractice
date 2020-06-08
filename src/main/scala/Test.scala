import java.sql.Timestamp
import java.util
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType, TimestampType}


object Test{

  var accumList=new ListAccum()

  def main(args: Array[String]): Unit = {

    val getEmpSchema=StructType(Array(StructField("emp_id",DataTypes.IntegerType),
      StructField("emp_name",DataTypes.StringType),
      StructField("salary",DataTypes.DoubleType),
      StructField("join_date",DataTypes.TimestampType)))

    val spark=SparkSession.builder()
      .master("local[*]")
    //  .enableHiveSupport()
      .getOrCreate();

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);


   val empData= spark
      .read
      .option("header", "true")
      .schema(getEmpSchema)
      .csv("src/main/resources/emp_joindate.csv");

    val groupedData=empData
      .orderBy(functions.asc("join_date"))
      .groupBy("emp_id")
      .agg(functions.collect_list(empData.col("join_date")).as("list_join_dates"))


    spark.sparkContext.register(accumList);

   val validgroupedData= groupedData
     .withColumn("valid_join_dates", getValidSvcDatesUDF(groupedData.col("list_join_dates")))

   val empDataWithSvcDt= validgroupedData
      .withColumn("svc_start_dt",explode(col("valid_join_dates")))
      .drop("list_join_dates", "valid_join_dates").dropDuplicates
    empDataWithSvcDt.printSchema()
    empDataWithSvcDt.show(false)



  }

  val getValidSvcDatesUDF=udf(getValidSvcDates)

  def getValidSvcDates() = (svcDates:Seq[Timestamp])=>{

    accumList.reset()

  //  var validJoinDates:List[Timestamp]=List.empty
    for (i <- svcDates.indices) {
      val joinDT = svcDates(i)

     // println("length:"+accumvar.)
      if (svcDates.length == 1)
        accumList.add(joinDT)
      else {
        var recentTS = if (i == 0) joinDT
        else  accumList.lastValue()

        val cal = Calendar.getInstance
        cal.setTime(recentTS)
        cal.add(Calendar.DAY_OF_WEEK, 30)
        val time_30 = new Timestamp(cal.getTime.getTime)
        if (recentTS.before(joinDT) && time_30.after(joinDT)) {
          println("inside if")
          accumList.add(recentTS)
        }
        else {
          println("inside else")
          accumList.add(joinDT)
        }
      }
    }
    accumList.getList();

  }


}
