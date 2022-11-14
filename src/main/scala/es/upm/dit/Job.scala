/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.upm.dit

import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import utils.DemoStreamEnvironment
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}

object Job {
  def main(args: Array[String]) {

    val hostname: String = "localhost"
    val port: Int = 9095

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream(hostname, port)

    // set up the execution environment
    //val env: StreamExecutionEnvironment = DemoStreamEnvironment.env
    //val text = env.socketTextStream("localhost", 9092)

    // val textApplied = text.flatMapWith {case (_, sequences) => sequences}

    data.print()

    // The execute() method will wait for the job to finish and then return a JobExecutionResult,
    // this contains execution times and accumulator results.
    env.execute("SocketEventStreamer")


// https://stackoverflow.com/questions/54985953/parsing-json-from-incoming-datastream-to-perform-simple-transformations-in-flink


  }
}
