package com.baidu.spark.eval.func;

import com.google.common.collect.ImmutableSet;

public interface FunctionsProvider {

    /**
     * @return incompatible functions in lower case
     */
    ImmutableSet<String> getInCompatibleFunctions();
}
