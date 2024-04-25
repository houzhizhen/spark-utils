package com.baidu.spark.eval

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_FUNCTION

import scala.collection.mutable
import scala.collection.mutable.Set

class Utils {
  def findFunctions(plan: LogicalPlan): Set[String] = {
    val functions = Set[String]()

    plan.resolveExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
      case f @ UnresolvedFunction(nameParts, _, _, _, _) =>
        functions += nameParts.mkString(".")
        f
    }
    functions
  }
}
