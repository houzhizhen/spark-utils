package org.apache.spark.sql.execution;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

public class SparkSqlParserUtil {
    /**
     *  Since SparkSqlParser is not public, we need to this indirect call to the parsePlan method
     * @param sql
     * @return
     */
    public static LogicalPlan parsePlan(String sql) {
        return new SparkSqlParser().parsePlan(sql);
    }

    public static List<String> findFunctions(LogicalPlan plan) {
        List<String> functions = new ArrayList<>();
        return functions;
    }
}
