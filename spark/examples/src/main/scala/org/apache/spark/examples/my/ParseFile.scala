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

object ParseFile {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: ParseFile <source_file> " +
        "<output_filename> [<iter>]")
      System.exit(1)
    }
    val source_file = args(0)
    val output_filename = args(1)

    val spark = SparkSession.builder()
      .appName("File Transform")
      .getOrCreate()

    val sc = spark.sparkContext
// 读取输入文
    val input = sc.textFile(source_file)

    // 使用flatMap将每行数据转换为新的格式
    val output = input.map(line => {
      val strings = line.split(",")
      strings(0)+ " " + strings(1)
    }).repartition(1)
    // 将输出保存到文件中
    output.saveAsTextFile(output_filename)
  }
}
// scalastyle:on println
