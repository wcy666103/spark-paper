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

import org.apache.spark.{SparkConf, SparkContext}

object RandomSample {
  def main(args: Array[String]): Unit = {
    // 设置Spark应用程序的名称
    val appName = "RandomSample"
    // 初始化Spark配置
    val conf = new SparkConf().setAppName(appName).setMaster("local[20]")
    // 创建SparkContext
    val sc = new SparkContext(conf)

    // 读取大文件
    val inputFile = "/home/parse/wcy/data/parse"
//    val inputFile = "hdfs://node2:9820/wcy_data/Graph2/followers-2.10y.txt"
    val inputRDD = sc.textFile(inputFile)

    // 获取文件总行数
    val totalLines = inputRDD.count()

    // 设置需要随机选择的行数（这里设置为1亿）
    val sampleSize = 15000000

    // 计算每个分区需要抽样的数量
    val samplesPerPartition = sampleSize / totalLines.toDouble

    // 使用sample方法进行随机抽样
    val sampledRDD = inputRDD
      .mapPartitions(iter => iter.filter(_ => Math.random() < samplesPerPartition))
//      .coalesce((sampleSize / 5000000).toInt)  // 5000000为一个分区大小
      .repartition(1)

    // 将抽样结果保存到新文件
    val outputFile = "/home/parse/sample"
    sampledRDD.saveAsTextFile(outputFile)

    // 关闭SparkContext
    sc.stop()
  }
}