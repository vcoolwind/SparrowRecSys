package com.stone.study.spark;

public class FileUtils {
    public static String getClassPathFile(String file){
       return FileUtils.class.getClassLoader().getResource(file).getPath();
    }
}
