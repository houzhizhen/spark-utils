package com.baidu.spark.eval;

public class EvalResult {

    private final EvalStatus status;
    private final String diagnostics;

    public EvalResult(EvalStatus status) {
        this(status, null);
    }
    public EvalResult(EvalStatus status, String diagnostics) {
        this.status = status;
        this.diagnostics = diagnostics;
    }
    public EvalStatus getStatus() {
        return status;
    }

    public String getDiagnostics() {
        return diagnostics;
    }
}
