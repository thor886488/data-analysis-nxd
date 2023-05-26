package com.analysis.nxd.common.utils;

import java.util.List;

public class ListArrUtils {


    /**
     * 包含的索引
     *
     * @param list
     * @return
     */
    public static int getIndex(List list, String cell) {
        if (list != null && list.size() > 0) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).toString().trim().equals(cell)) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * 是否包含
     *
     * @param list
     * @return
     */
    public static boolean isContain(List list, String cell) {
        if (list != null && list.size() > 0) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).toString().trim().equals(cell)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 包含的索引
     *
     * @return
     */
    public static int getIndex(String[] arr, String cell) {
        if (arr != null && arr.length > 0) {
            for (int i = 0; i < arr.length; i++) {
                if (arr[i].toString().trim().equals(cell)) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * 是否包含
     *
     * @return
     */
    public static boolean isContain(String[] arr, String cell) {
        if (arr != null && arr.length > 0) {
            for (int i = 0; i < arr.length; i++) {
                if (arr[i].toString().trim().equals(cell)) {
                    return true;
                }
            }
        }
        return false;
    }
}
