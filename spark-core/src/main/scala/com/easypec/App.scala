package com.easypec

/**
 * Hello world!
 *
 */
object App extends App {

  import org.apache.spark.SparkContext
  import org.apache.spark.sql.SQLContext
  import org.apache.phoenix.spark._

  val rdd1 = sc.phoenixTableAsRDD(
    "EASYPEC_WEBLOG.DM_001_DAT_VISIT_DETAIL",
    Seq("DAT_VISIT_DETAIL_ID", "PV", "UV", "DISTINCT_UV", "LANDING_STATE", "EXTERNAL_LINKS", "SEARCH_ENGINES", "SEARCH_WORD", "DIRECT_ACCESS", "LOCAL_TIME", "TERMINAL", "BROWSR_TYPE", "USER_ID", "COOKIE_ID", "COMPANY_ID", "PROVINCE", "CITY", "VISIT_SOURCE_INOUT",
      "VISIT_TYPE", "PLATE", "COMPANY", "ETL_TIME", "SEQUENCENO"),
    Some("DAT_VISIT_DETAIL_ID <= '--2016-04-15'"),
    zkUrl=Some("BigData06,BigData07,BigData08,BigData09,BigData10"))


}
