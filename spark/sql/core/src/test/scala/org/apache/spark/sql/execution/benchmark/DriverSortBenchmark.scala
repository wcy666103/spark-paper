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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.internal.SQLConf

import java.io.{FileWriter, OutputStreamWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * DriverSort benchmark.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/DriverSortBenchmark-results.txt".
 * }}}
 */
object DriverSortBenchmark extends SqlBasedBenchmark {

  private def runDriverSort(row: Long, map: mutable.Map[Long, ArrayBuffer[String]],outget: Option[OutputStreamWriter]): Unit = {
    val df = spark.range(row).selectExpr("id % 7 as c").repartition(200)

    val benchmark = new Benchmark("DriverSort", row, output = output, resultss = map,outget = outget)

    benchmark.addCase("Sort", 5) { _ =>
      withSQLConf(SQLConf.DRIVER_SORT_THRESHOLD.key -> "0") {
        df.sort("c").collect()
      }
    }
    benchmark.addCase("DriverSort", 5) { _ =>
      withSQLConf(SQLConf.DRIVER_SORT_THRESHOLD.key -> row.toString) {
        df.sort("c").collect()
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    spark.sparkContext.getConf.setMaster("local[*]")
    val output = new java.io.FileWriter("/home/project/local-sort/spark/result.txt")
    val getout = Option(output)
    runBenchmark("DriverSortBenchmark") {
      val map = mutable.Map[Long, ArrayBuffer[String]]()
      var count = 100000
      while (count <= 8000000) {
        runDriverSort(count, map,getout)
        count += 100000
      }
      println(map)

      val all = new FileWriter("/home/project/local-sort/spark/allresult")
      for {
        (key, values) <- map
        value <- values
      } {
        all.write(s"$key,$value\n")
      }
      all.close()
    }
    output.close()
  }
}
