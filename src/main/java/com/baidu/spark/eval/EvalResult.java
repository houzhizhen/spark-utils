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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EvalResult(")
                .append("status=")
                .append(status.name());
        if (diagnostics != null){
            sb.append(", diagnostics=").append(diagnostics);
        }
        sb.append(")");
        return sb.toString();
    }
}
