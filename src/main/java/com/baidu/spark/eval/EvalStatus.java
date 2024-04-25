package com.baidu.spark.eval;

public enum EvalStatus {
    SUCCESS, // 在 Spark 里可以执行
    SYNTAX_INCOMPATIBLE, // 语法不兼容
    FUNCTION_INCOMPATIBLE, // 函数不兼容
    FUNCTION_NOT_FOUND,
    NOT_EVAL, // 未执行解析
    SET,
    ADD // NOT separate ADD FILE, ADD JAR, ADD ARCHIVE
}
