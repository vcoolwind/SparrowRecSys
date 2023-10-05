package com.sparrowrecsys.online.datamanager;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Stone
 */
public class DataFileUtil {

    private static final Map<String, String> DATA_FILE_MAP = new HashMap<String, String>();
    public static final String MOVIES = "movies";
    public static final String LINKS = "links";
    public static final String RATINGS = "ratings";
    public static final String ITEM2VEC_EMB = "item2vecEmb";
    public static final String USER_EMB = "userEmb";

    static {
        String rootPath = DataFileUtil.class.getResource("/").getPath() + "webroot/";
        DATA_FILE_MAP.put(MOVIES, rootPath + String.format("sampledata/%s.csv",MOVIES ));
        DATA_FILE_MAP.put(LINKS, rootPath + String.format("sampledata/%s.csv",LINKS ));
        DATA_FILE_MAP.put(RATINGS, rootPath + String.format("sampledata/%s.csv",RATINGS ));
        DATA_FILE_MAP.put(ITEM2VEC_EMB, rootPath + String.format("modeldata/%s.csv",ITEM2VEC_EMB ));
        DATA_FILE_MAP.put(USER_EMB, rootPath + String.format("modeldata/%s.csv",USER_EMB ));
    }
    public static String get(String key){
        return  DATA_FILE_MAP.get(key);
    }
    public static void main(String[] args){
        DATA_FILE_MAP.entrySet().forEach(entry ->System.out.println(entry));
    }
}
