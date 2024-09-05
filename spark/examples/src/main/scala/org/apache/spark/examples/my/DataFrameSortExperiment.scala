/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.my
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object DataFrameSortExperiment {
  def main(args: Array[String]): Unit = {
    val conf = args(0).toInt
    // 创建SparkSession
    val spark = SparkSession.builder
      .config(SQLConf.DRIVER_SORT_THRESHOLD.key,conf.toString)
      .getOrCreate()

    val scale = args(1).toInt
    val range = args(2).toInt
    // 生成测试数据
    import spark.implicits._
//    val data = Seq.fill(scale)((scala.util.Random.nextInt(range), "value")).toDF("key", "value")
    val data = spark.range(1, scale)
                .selectExpr(s"CAST(rand() * ${range} AS INT) as key", "'value' as value")

    // 正常排序
    val sortedDataNormal = data.sort("key").filter($"key" % 2 =!= 0).groupBy("value").count()
    sortedDataNormal.collect().foreach(println)

    // 关闭SparkSession
    spark.stop()
  }
}