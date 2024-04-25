package com.baidu.spark.eval;

import com.baidu.spark.eval.func.FileFunctionsProvider;
import com.baidu.spark.util.rand.ConfUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParserUtil;
import scala.collection.Iterator;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class SparkSQLEvaluate {

    public static final Logger LOG = Logger.getLogger(SparkSQLEvaluate.class);
    private static String SPARK_QUERY_TO_EVALUATE = "spark.query.to.evaluate";
    private final SparkSession session;
    private final ImmutableSet<String> incompatibleFuncs;

    public SparkSQLEvaluate(SparkSession session, ImmutableSet<String> incompatibleFuncs) {
        this.session = session;
        this.incompatibleFuncs = incompatibleFuncs;
        LOG.info("incompatibleFuncs:" + incompatibleFuncs);
    }

    protected SparkSQLEvaluate() {
         this(SparkSession.builder()
                .appName("TestSql").master("local[1]").getOrCreate(),
              ImmutableSet.of());
    }

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
        Set<String> inCompatibleFuncsSet = new HashSet<>();
        Set<String> notFoundFuncs = new HashSet<>();
        while (functions.hasNext()) {
            String funcName = functions.next();
            if (! session.catalog().functionExists(funcName)) {
                notFoundFuncs.add(funcName);
            }
            if (this.incompatibleFuncs.contains(funcName)) {
                inCompatibleFuncsSet.add(funcName);
            }
        }
        if (! inCompatibleFuncsSet.isEmpty()) {
            return new EvalResult(EvalStatus.FUNCTION_INCOMPATIBLE,
                    inCompatibleFuncsSet.toString());
        }
        if (! notFoundFuncs.isEmpty()) {
            return new EvalResult(EvalStatus.FUNCTION_NOT_FOUND,
                    notFoundFuncs.toString());
        }

        return new EvalResult(EvalStatus.SUCCESS, null);
    }

    private EvalResult evalHiveCommand(HiveCommand hiveCommand) {
        switch (hiveCommand) {
            case SET:
                return new EvalResult(EvalStatus.SET);
            case ADD:
                return new EvalResult(EvalStatus.ADD);
            default:
                return new EvalResult(EvalStatus.NOT_EVAL, null);
        }
    }

    public static void main(String[] args) {
        Configuration conf = ConfUtils.getConf(args);
        String sql = conf.get(SPARK_QUERY_TO_EVALUATE);
        if (sql == null) {
            LOG.error("Must pass parameter " + SPARK_QUERY_TO_EVALUATE);
            return;
        }
        SparkSession session = SparkSession.builder()
                .appName("TestSql")
                .master("local[1]")
                .getOrCreate();
        ImmutableSet incompatibleFuncs = new FileFunctionsProvider().getInCompatibleFunctions();
        try {
            EvalResult result = new SparkSQLEvaluate(session, incompatibleFuncs)
                    .evaluate(sql);
            LOG.info("result:" + result);
        } finally {
            session.close();
        }
    }
}
