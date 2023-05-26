package com.analysis.nxd.common.utils;

public class OSUtils {
    public static boolean isWin(){
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("win") || os.toLowerCase().startsWith("mac")) {
            return true;
        }else {
            return false;
        }
    }

}
