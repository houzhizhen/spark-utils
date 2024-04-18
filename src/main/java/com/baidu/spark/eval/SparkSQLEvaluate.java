package com.baidu.spark.eval;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParserUtil;
import scala.collection.Iterator;

import java.util.Locale;

public class SparkSQLEvaluate {

    public static final Logger LOG = Logger.getLogger(SparkSQLEvaluate.class);

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
            return evalHiveCommand(hiveCommand);
        }
        LogicalPlan plan = null;
        try {
            plan = SparkSqlParserUtil.parsePlan(cmdTrimmed);
        } catch (Exception e) {
            return new EvalResult(EvalStatus.SYNTAX_INCOMPATIBLE, e.getMessage());
        }
        // Find functions
       Iterator<String> functions = new Utils().findFunctions(plan).toIterator();
        while (functions.hasNext()) {
            String function = functions.next();
            LOG.info("Function: " + function);
        }

        return new EvalResult(EvalStatus.SUCCESS, null);
    }

    private EvalResult evalHiveCommand(HiveCommand hiveCommand) {
        switch (hiveCommand) {
            case SET:
                return new EvalResult(EvalStatus.SET);
            default:
                return new EvalResult(EvalStatus.NOT_EVAL, null);
        }
    }
}
