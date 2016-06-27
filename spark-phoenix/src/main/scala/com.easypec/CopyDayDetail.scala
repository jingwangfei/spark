package com.easypec

import org.apache.spark.SparkConf

/**
  * Created by jingtao on 2016/6/21.
  */
object CopyDayDetail {

  /*
   nohup spark-submit --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=2048M -XX:MaxDirectMemorySize=1536M"
   --conf "spark.yarn.executor.memoryOverhead=4096" --conf "spark.akka.frameSize=100"
   --driver-memory 1g --master yarn-client --executor-memory 4g --executor-cores 4
   --num-executors 2 spark0100.jar > copy.log 2<&1 &
   */
  def main(args: Array[String]) {
    import org.apache.phoenix.spark._
    import org.apache.spark.SparkContext

    val conf = new SparkConf().setAppName("copy_spark").set("spark.rpc.askTimeout", "300").set("spark.akka.timeout", "300")
      .set("spark.core.connection.ack.wait.timeout", "300").set("spark.shuffle.io.connectionTimeout", "300")
      .set("spark.reducer.maxSizeInFlight", "24")
    val sc = new SparkContext(conf)

    val rdd1 = sc.phoenixTableAsRDD(
      "EASYPEC_WEBLOG.DM_001_DAT_VISIT_DETAIL_2",
      Array("DAT_VISIT_DETAIL_ID", "PV", "UV", "DISTINCT_UV", "LANDING_STATE", "EXTERNAL_LINKS", "SEARCH_ENGINES",
        "SEARCH_WORD", "DIRECT_ACCESS", "LOCAL_TIME", "TERMINAL", "BROWSR_TYPE", "USER_ID", "COOKIE_ID", "COMPANY_ID",
        "PROVINCE", "CITY", "VISIT_SOURCE_INOUT", "VISIT_TYPE", "PLATE", "COMPANY", "ETL_TIME", "SEQUENCENO"),
      zkUrl = Some("BigData06,BigData07,BigData08,BigData09,BigData10"))

    val rdd11 = rdd1.map(detail => (detail("SEQUENCENO"), detail)).groupByKey().map(tuple => tuple._2.head)

    rdd11.cache()

    rdd11.map(entry => (entry("DAT_VISIT_DETAIL_ID"), entry("PV"), entry("UV"), entry("DISTINCT_UV"), entry("LANDING_STATE"), entry("EXTERNAL_LINKS"), entry("SEARCH_ENGINES"), entry("SEARCH_WORD"), entry("DIRECT_ACCESS"), entry("LOCAL_TIME"), entry("TERMINAL"), entry("BROWSR_TYPE"), entry("USER_ID"), entry("COOKIE_ID"), entry("COMPANY_ID"), entry("PROVINCE"), entry("CITY"), entry("VISIT_SOURCE_INOUT"), entry("VISIT_TYPE"), entry("PLATE"), entry("COMPANY"), entry("ETL_TIME"))).saveToPhoenix("EASYPEC_WEBLOG.DM_001_DAT_VISIT_DETAIL", Seq("DAT_VISIT_DETAIL_ID", "PV", "UV", "DISTINCT_UV", "LANDING_STATE", "EXTERNAL_LINKS", "SEARCH_ENGINES", "SEARCH_WORD", "DIRECT_ACCESS", "LOCAL_TIME", "TERMINAL", "BROWSR_TYPE", "USER_ID", "COOKIE_ID", "COMPANY_ID", "PROVINCE", "CITY", "VISIT_SOURCE_INOUT", "VISIT_TYPE", "PLATE", "COMPANY", "ETL_TIME"), zkUrl = Some("BigData06,BigData07,BigData08,BigData09,BigData10"))

    rdd11.map(entry => (entry("DAT_VISIT_DETAIL_ID"), entry("SEQUENCENO"))).saveToPhoenix("EASYPEC_WEBLOG.DM_001_DAT_VISIT_DETAIL_2", Seq("DAT_VISIT_DETAIL_ID", "SEQUENCENO"), zkUrl = Some("BigData06,BigData07,BigData08,BigData09,BigData10"))
  }
}
