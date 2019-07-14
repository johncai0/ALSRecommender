package org.john.local

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


/**
  * Created by root on 18-5-9.
  */
object userTarget {
  def main(args: Array[String]) {
    val ss=SparkSession.builder().getOrCreate()
    val base = "hdfs://john:9000/user/ALSRecommender_TrainingData/*"

    val fieldSchema = StructType(Array(
      StructField("user", StringType, true),
      StructField("itemrank", IntegerType, true),
      StructField("item", StringType, true)
    ))
    val df=ss.read.schema(fieldSchema).csv(base)
    val recommend=new ALSrecommend(ss,df)
    recommend.evaluate()
    recommend.makeRecommendations("3cd6d42a-4cba-1a22-181c-e4d3ca558e19",5).show(false)

    //============================================up main=======================================================================
  }
  /*
  * 文本结构化类
  * */
}

