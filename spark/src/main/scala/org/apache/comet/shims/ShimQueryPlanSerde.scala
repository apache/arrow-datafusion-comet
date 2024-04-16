/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, BinaryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate

trait ShimQueryPlanSerde {
  def getFailOnError(b: BinaryArithmetic): Boolean =
    b.getClass.getMethod("failOnError").invoke(b).asInstanceOf[Boolean]

  def getFailOnError(aggregate: DeclarativeAggregate): Boolean = {
    val failOnError = aggregate.getClass.getDeclaredMethods.flatMap(m =>
      m.getName match {
        case "failOnError" | "useAnsiAdd" => Some(m.invoke(aggregate).asInstanceOf[Boolean])
        case _ => None
      })
    if (failOnError.isEmpty) {
      aggregate.getClass.getDeclaredMethods
        .flatMap(m =>
          m.getName match {
            case "initQueryContext" => Some(m.invoke(aggregate).asInstanceOf[Option[_]].isDefined)
            case _ => None
          })
        .head
    } else {
      failOnError.head
    }
  }

  // TODO: delete after drop Spark 3.2/3.3 support
  // This method is used to check if the aggregate function is in legacy mode.
  // EvalMode is an enum object in Spark 3.4.
  def isLegacyMode(aggregate: DeclarativeAggregate): Boolean = {
    val evalMode = aggregate.getClass.getDeclaredMethods
      .flatMap(m =>
        m.getName match {
          case "evalMode" => Some(m.invoke(aggregate))
          case _ => None
        })

    if (evalMode.isEmpty) {
      true
    } else {
      // scalastyle:off caselocale
      evalMode.head.toString.toLowerCase == "legacy"
      // scalastyle:on caselocale
    }
  }

  // TODO: delete after drop Spark 3.2 support
  def isBloomFilterMightContain(binary: BinaryExpression): Boolean = {
    binary.getClass.getName == "org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain"
  }
}
