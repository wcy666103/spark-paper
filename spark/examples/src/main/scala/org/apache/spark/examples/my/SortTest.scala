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

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.sql.SparkSession


/** Computes an approximation to pi */
object SortTest {
  def main(args: Array[String]): Unit = {
    val size = args(0).toInt
    val spark = SparkSession
      .builder()
      .appName("SortTest")
//      .master("local[*]")
      .config("spark.driver.memory","1g")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      .getOrCreate()

    val start = System.currentTimeMillis()
    val sc = spark.sparkContext
    sc.textFile("hdfs://node2:9820/wcy_data/Wordcount/" + size + "g")
      .flatMap(_.split(" ")).map(w => (w, 1)).
      reduceByKey(_ + _, 1)
      .asInstanceOf[ShuffledRDD[String, Int, Int]]
      .setKeyOrdering(Ordering.String)
      .saveAsTextFile("/home/project/local-sort/spark/resultsss")
//      .collect()
//      .foreach(println)
    val end = System.currentTimeMillis()
    println("程序总耗费时间：" + (end - start) / 1000)
//    Thread.sleep(1000000)
    spark.stop()
  }
}
// scalastyle:on println
