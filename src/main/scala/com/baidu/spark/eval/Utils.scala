package com.baidu.spark.eval

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable.Set

class Utils {
  def findFunctions(plan: LogicalPlan): Set[String] = {
    var functions = Set[String]()
    plan.resolveExpressions {
      case f@UnresolvedFunction(nameParts, _, _, _, _) =>
        functions.+(nameParts.mkString("."))
        f
    }
    functions
  }
}
