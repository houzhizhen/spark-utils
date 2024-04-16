package com.baidu.spark.eval;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParserUtil;

import java.util.List;
import java.util.Locale;

public class SparkSQLEvaluate {

    /**
     *
     * @param cmd
     * @return
     */
    public EvalResult evaluate(String cmd) {
        Preconditions.checkNotNull(cmd, "cmd is null");
        String cmdTrimmed = cmd.trim();
        String cmdLower = cmdTrimmed.toLowerCase(Locale.ROOT);
        String[] tokens = cmdLower.split("\\s+");
        String cmd1  = cmdTrimmed.substring(tokens[0].length()).trim();
        if ("quit".equals(cmdLower) ||
            "exit".equals(cmdLower) ||
            "source".equals(tokens[0]) ||
            cmdTrimmed.startsWith("!")) {
            return new EvalResult(EvalStatus.NOT_EVAL, null);
        }
        HiveCommand hiveCommand = HiveCommand.find(tokens, false);
        if (hiveCommand != null) {
            return new EvalResult(EvalStatus.NOT_EVAL, null);
        }
        LogicalPlan plan = null;
        try {
            plan = SparkSqlParserUtil.parsePlan(cmdTrimmed);
        } catch (Exception e) {
            return new EvalResult(EvalStatus.SYNTAX_INCOMPATIBLE, e.getMessage());
        }
        // Find functions
       List<String> functions = SparkSqlParserUtil.findFunctions(plan);

        return new EvalResult(EvalStatus.SUCCESS, null);
    }
}
