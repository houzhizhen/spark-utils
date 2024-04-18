package org.apache.spark.sql.execution;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class SparkSqlParserUtil {
    /**
     * Since SparkSqlParser is not public, we need to this indirect call to the parsePlan method
     */
    public static LogicalPlan parsePlan(String sql) {
        return new SparkSqlParser().parsePlan(sql);
    }
}
