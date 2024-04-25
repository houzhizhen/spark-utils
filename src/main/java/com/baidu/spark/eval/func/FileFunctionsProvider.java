package com.baidu.spark.eval.func;

import com.google.common.collect.ImmutableSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class FileFunctionsProvider implements FunctionsProvider {

    /**
     * @return incompatible functions in lower case
     */
    @Override
    public ImmutableSet<String> getInCompatibleFunctions() {
        Set<String> funcSet = new HashSet<>();
        File file = new File("incompatible-functions.txt");
        if (file.exists()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String str = null;
                while((str = br.readLine()) != null){
                    funcSet.add(str.trim().toLowerCase(Locale.ROOT));
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ImmutableSet.copyOf(funcSet);
    }
}
