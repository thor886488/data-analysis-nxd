package com.analysis.nxd.common.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author ：kequan
 * @date ：Created in 2019/12/3 10:13
 * @description：
 * @modified By：
 */
public class FileUtils {

    public static void deleteFile(String fileName) {
        try {
            File file = new File(fileName);
            if (file.exists()) {
                file.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void appendToFile(String fileName, String str) {
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            writer.append(str);
            writer.newLine();
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void appendListToFile(String fileName, List<String> list) {
        try {
            File file = new File(fileName);
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
            for (String str : list) {
                writer.append(str);
                writer.newLine();
                writer.flush();
            }
            System.out.println("写入文件条数：" + list.size());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
