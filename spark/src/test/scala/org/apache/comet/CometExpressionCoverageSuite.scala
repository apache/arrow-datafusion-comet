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

package org.apache.comet

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.sys.process._

import org.scalatest.Ignore
import org.scalatest.exceptions.TestFailedException

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Manual test to calculate Spark builtin expressions coverage support by the Comet
 *
 * The test will update files doc/spark_builtin_expr_coverage.txt,
 * doc/spark_builtin_expr_coverage_agg.txt
 */
@Ignore
class CometExpressionCoverageSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  private val rawCoverageFilePath = "doc/spark_builtin_expr_coverage.txt"
  private val aggCoverageFilePath = "doc/spark_builtin_expr_coverage_agg.txt"
  private val rawCoverageFileDatafusionPath = "doc/spark_builtin_expr_df_coverage.txt"

  private val queryPattern = """(?i)SELECT (.+?);""".r
  private val valuesPattern = """(?i)FROM VALUES(.+?);""".r
  private val selectPattern = """(i?)SELECT(.+?)FROM""".r

  def getExamples(): Map[String, List[String]] =
    spark.sessionState.functionRegistry
      .listFunction()
      .map(spark.sessionState.catalog.lookupFunctionInfo(_))
      .filter(_.getSource.toLowerCase == "built-in")
      // exclude spark streaming functions, Comet has no plans to support streaming in near future
      .filter(f =>
        !List("window", "session_window", "window_time").contains(f.getName.toLowerCase))
      .map(f => {
        val selectRows = queryPattern.findAllMatchIn(f.getExamples).map(_.group(0)).toList
        (f.getName, selectRows.filter(_.nonEmpty))
      })
      .toMap

  /**
   * Manual test to calculate Spark builtin expressions coverage support by the Comet
   *
   * The test will update files doc/spark_builtin_expr_coverage.txt,
   * doc/spark_builtin_expr_coverage_agg.txt
   */
  test("Test Spark builtin expressions coverage") {
    val builtinExamplesMap = getExamples()

    // key - function name
    // value - list of result shows if function supported by Comet
    val resultsMap = new mutable.HashMap[String, CoverageResult]()

    builtinExamplesMap.foreach {
      case (funcName, q :: _) =>
        val queryResult =
          try {
            // Example with predefined values
            // e.g. SELECT bit_xor(col) FROM VALUES (3), (5) AS tab(col)
            // better option is probably to parse the query and iterate through expressions
            // but this is adhoc coverage test
            if (q.toLowerCase.contains(" from values")) {
              val select = selectPattern.findFirstMatchIn(q).map(_.group(0))
              val values = valuesPattern.findFirstMatchIn(q).map(_.group(0))
              (select, values) match {
                case (Some(s), Some(v)) =>
                  testSingleLineQuery(s"select * $v", s"$s tbl")

                case _ =>
                  sys.error("Cannot parse properly")
              }
            } else {
              // Process the simple example like `SELECT cos(0);`
              //
              // The execution disables constant folding. This optimization rule precomputes and selects the value as literal
              // which subsequently leads to false positives
              //
              // ConstantFolding is a operator optimization rule in Catalyst that replaces expressions
              // that can be statically evaluated with their equivalent literal values.
              testSingleLineQuery(
                "select 'dummy' x",
                s"${q.dropRight(1)}, x from tbl",
                excludedOptimizerRules =
                  Some("org.apache.spark.sql.catalyst.optimizer.ConstantFolding"))
            }
            CoverageResult(CoverageResultStatus.Passed.toString, Seq((q, "OK")))
          } catch {
            case e: TestFailedException
                if e.message.getOrElse("").contains("Expected only Comet native operators") =>
              CoverageResult(CoverageResultStatus.Failed.toString, Seq((q, "Unsupported")))
            case e if e.getMessage.contains("CometNativeException") =>
              CoverageResult(
                CoverageResultStatus.Failed.toString,
                Seq((q, "Failed on native side")))
            case _ =>
              CoverageResult(
                CoverageResultStatus.Failed.toString,
                Seq((q, "Failed on something else. Check query manually")))
          }
        resultsMap.put(funcName, queryResult)

      case (funcName, List()) =>
        resultsMap.put(
          funcName,
          CoverageResult(
            CoverageResultStatus.Skipped.toString,
            Seq(("", "No examples found in spark.sessionState.functionRegistry"))))
    }

    // TODO: convert results into HTML or .md file
    resultsMap.toSeq.toDF("name", "details").createOrReplaceTempView("t")
    val str_agg = showString(
      spark.sql(
        "select result, d._2 as details, count(1) cnt from (select name, t.details.result, explode_outer(t.details.details) as d from t) group by 1, 2 order by 1"),
      1000,
      0)
    Files.write(Paths.get(aggCoverageFilePath), str_agg.getBytes(StandardCharsets.UTF_8))

    val str = showString(spark.sql("select * from t order by 1"), 1000, 0)
    Files.write(Paths.get(rawCoverageFilePath), str.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Manual test to calculate Spark builtin expressions coverage support by the Datafusion
   * directly
   *
   * The test will update files doc/spark_builtin_expr_df_coverage.txt,
   *
   * Requires to set DATAFUSIONCLI_PATH env variable to valid path to datafusion-cli
   */
  test("Test Spark builtin expressions coverage by Datafusion directly") {
    val builtinExamplesMap = getExamples()

    // key - function name
    // value - list of result shows if function supported by Datafusion
    val resultsMap = new mutable.HashMap[String, CoverageResult]()

    builtinExamplesMap.foreach {
      case (funcName, q :: _) =>
        val queryResult =
          try {
            // Example with predefined values
            // e.g. SELECT bit_xor(col) FROM VALUES (3), (5) AS tab(col)
            if (q.toLowerCase.contains(" from values")) {
              val select = selectPattern.findFirstMatchIn(q).map(_.group(0))
              val values = valuesPattern.findFirstMatchIn(q).map(_.group(0))
              (select, values) match {
                case (Some(s), Some(v)) =>
                  withTempDir { dir =>
                    val path = new Path(dir.toURI.toString).toUri.getPath
                    spark.sql(s"select * $v").repartition(1).write.mode("overwrite").parquet(path)
                    runDatafusionCli(s"""$s '$path/*.parquet'""")
                  }

                case _ =>
                  sys.error("Cannot parse properly")
              }
            } else {
              // Process the simple example like `SELECT cos(0);`
              runDatafusionCli(q)
            }
            CoverageResult(CoverageResultStatus.Passed.toString, Seq((q, "OK")))
          } catch {
            case e: Throwable =>
              CoverageResult(CoverageResultStatus.Failed.toString, Seq((q, e.getMessage)))
          }
        resultsMap.put(funcName, queryResult)

      case (funcName, List()) =>
        resultsMap.put(
          funcName,
          CoverageResult(
            CoverageResultStatus.Skipped.toString,
            Seq(("", "No examples found in spark.sessionState.functionRegistry"))))
    }

    resultsMap.toSeq.toDF("name", "details").createOrReplaceTempView("t")
    val str = showString(spark.sql("select * from t order by 1"), 1000, 0)
    Files.write(Paths.get(rawCoverageFileDatafusionPath), str.getBytes(StandardCharsets.UTF_8))
  }

  private def runDatafusionCli(sql: String) = {

    val datafusionCliPath = sys.env.getOrElse(
      "DATAFUSIONCLI_PATH",
      sys.error("DATAFUSIONCLI_PATH env variable not set"))

    val tempFilePath = Files.createTempFile("temp-", ".sql")
    try {
    Files.write(tempFilePath, sql.getBytes)
   
    val command = s"""$datafusionCliPath/datafusion-cli -f $tempFilePath"""

    val stdout = new StringBuilder
    val stderr = new StringBuilder

    command.!(
      ProcessLogger(
        out => stdout.append(out).append("\n"), // stdout
        err => stderr.append(err).append("\n") // stderr
      ))
   } finally {
      Files.delete(tempFilePath)
   }

    val err = stderr.toString()
    val out = stdout.toString()

    if (err.nonEmpty)
      sys.error(s"std_err: $err")

    if (out.toLowerCase.contains("error"))
      sys.error(s"std_out: $out")
  }
}

case class CoverageResult(result: String, details: Seq[(String, String)])

object CoverageResultStatus extends Enumeration {
  type CoverageResultStatus = Value

  val Failed: Value = Value("FAILED")
  val Passed: Value = Value("PASSED")
  val Skipped: Value = Value("SKIPPED")
}
