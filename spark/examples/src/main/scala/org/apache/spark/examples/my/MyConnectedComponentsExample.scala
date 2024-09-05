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

// $example on$
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
// $example off$

/**
 * A connected components algorithm example.
 * The connected components algorithm labels each connected component of the graph
 * with the ID of its lowest-numbered vertex.
 * For example, in a social network, connected components can approximate clusters.
 * GraphX contains an implementation of the algorithm in the
 * [`ConnectedComponents` object][ConnectedComponents],
 * and we compute the connected components of the example social network dataset.
 *
 * Run with
 * {{{
 * bin/run-example graphx.ConnectedComponentsExample
 * }}}
 */
object MyConnectedComponentsExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: MyConnectedComponentsExample <fllower_file> <user_file>" +
        "<output_filename> [<iter>]")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
//      .setAppName("MyConnectedComponentsExample")
    val fllower_file = args(0)
    val user_file = args(1)
    val output_path = args(2)

    val sc = new SparkContext(sparkConf)

    // $example on$
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, fllower_file)
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    cc.foreach(println)
    // Join the connected components with the usernames
    val users = sc.textFile(user_file).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
//    users.foreach(println)
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
//    println(ccByUsername.collect().mkString("\n"))
    ccByUsername.collect().foreach(println)
//    ccByUsername.saveAsTextFile(output_path)
    // $example off$
//    readBoolean()
    sc.stop()
  }
}
// scalastyle:on println
