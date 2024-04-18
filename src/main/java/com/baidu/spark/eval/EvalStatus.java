package com.baidu.spark.eval;

public enum EvalStatus {
    SUCCESS, // 在 Spark 里可以执行
    SYNTAX_INCOMPATIBLE, // 语法不兼容
    FUNCTION_INCOMPATIBLE, // 函数不兼容
    NOT_EVAL, // 未执行解析
    SET,
    ADD_JAR
}
