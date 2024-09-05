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

object DataFrameSortExperiment2 {
  def main(args: Array[String]): Unit = {
    val conf = args(0).toInt
    val scale = args(1).toInt
    // 创建SparkSession
    val spark = SparkSession.builder
      .config(SQLConf.DRIVER_SORT_THRESHOLD.key,conf.toString)
//      .appName("localsort-" + conf + '-' + scale)
      .getOrCreate()



    val df = spark.range(scale).selectExpr("id % 7 as c").repartition(200)

    df.sort("c").collect()
  }
}