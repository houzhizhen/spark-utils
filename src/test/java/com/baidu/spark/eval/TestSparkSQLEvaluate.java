package com.baidu.spark.eval;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSparkSQLEvaluate {

    @Test
    public void testSparkSQLEvaluateQuit() {
        assertEval("quit", EvalStatus.NOT_EVAL);
    }

    @Test
    public void testSparkSQLEvaluateExit() {
        assertEval("exit", EvalStatus.NOT_EVAL);
    }

    @Test
    public void testSparkSQLEvaluateSource() {
        assertEval("source a.sql", EvalStatus.NOT_EVAL);
    }

    private EvalResult eval(String sql) {
        return new SparkSQLEvaluate().evaluate(sql);
    }

    private void assertEval(String sql, EvalStatus status) {
        EvalResult result = eval(sql);
        assertEquals(status, result.getStatus());
    }
}
