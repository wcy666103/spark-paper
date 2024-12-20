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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import java.net.URI

object ReadSortData {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: ParseFile <source_file> " +
        "<output_filename> [<iter>]")
      System.exit(1)
    }
    val rangeSize = args(0).toInt
    val conf1 = args(1).toInt

    val conf = new Configuration
    val fs = FileSystem.newInstance(URI.create("hdfs://node2:9820"), conf)
    val it = fs.listFiles(new Path("/wcy_data/sort-data/"+rangeSize), true)

    // 定义Schema，与写入时的列名和数据类型匹配
    val schema: StructType = StructType(Seq(
      StructField("c", LongType, nullable = true)
    ))

    // 创建SparkSession
    val spark = SparkSession.builder
      //      .appName("localsort-" + conf + '-' + scale)
      .config(SQLConf.DRIVER_SORT_THRESHOLD.key,conf1.toString)
      .getOrCreate()

    while (it.hasNext) {
      val status = it.next()
      val p: Path = status.getPath

//      println(p.toString)
      if(p.toString.endsWith(".parquet")){
        val df = spark.read.schema(schema).parquet(p.toString).toDF().repartition(200)
        df.sort("c").collect()
        println("end!!!")
        readLine()
        spark.stop
      }

    }

  }
}
