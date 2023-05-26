package com.analysis.nxd.common.utils;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.mysql.cj.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GoogleSheetsUtils {

    private static final String APPLICATION_NAME = "Google Sheets for plg";
    private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";

    /**
     * Global instance of the scopes required by this quickstart.
     * If modifying these scopes, delete your previously saved tokens/ folder.
     */
    private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";

    /**
     * Creates an authorized Credential object.
     *
     * @param HTTP_TRANSPORT The network HTTP Transport.
     * @return An authorized Credential object.
     * @throws IOException If the credentials.json file cannot be found.
     */
    private static Credential getCredentials(NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = GoogleSheetsUtils.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    /**
     * @param spreadsheetId
     * @param range
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public static List<String> readGoogleSheet(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
        ValueRange response = service.spreadsheets().values()
                .get(spreadsheetId, range)
                .execute();
        List<List<Object>> values = response.getValues();

        List<String> list = new ArrayList<>();
        if (values == null || values.isEmpty()) {
            //System.out.println("No data found.");
        } else {
            for (int m = 1; m < values.size(); m++) {
                List<Object> row = values.get(m);
                String rowData = "";
                for (int i = 0; i < row.size(); i++) {
                    String cell = "";
                    if (row.size() > i) {
                        cell = row.get(i).toString();
                    }
                    rowData = rowData + "'" + cell + "'" + ",";
                }
                //System.out.println(rowData.substring(0, rowData.length() - 1));
                list.add(rowData.substring(0, rowData.length() - 1));
            }
        }
        return list;
    }




    /**
     * @param spreadsheetId
     * @param range
     * @param keyColumns    唯一键字段，用 ， 分开
     * @param Columns       需要读取的字段名称， 用 ，分开,必须传值
     * @return
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public static List<String> readGoogleSheet(String spreadsheetId, String range, String keyColumns, String Columns, String integerColumns) throws IOException, GeneralSecurityException {
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
        ValueRange response = service.spreadsheets().values()
                .get(spreadsheetId, range)
                .execute();
        List<List<Object>> values = response.getValues();

        List<String> list = new ArrayList<>();
        if (values == null || values.isEmpty()) {
            //System.out.println("No data found.");
        } else {
            List<Object> rowTitle = values.get(0);
            List<Integer> listKey = new ArrayList();
            List<Integer> listColumns = new ArrayList();
            List<Integer> listIntegerColumns = new ArrayList();

            //  key 排序
            if (!StringUtils.isNullOrEmpty(keyColumns)) {
                String[] splitColumns = keyColumns.split(",");
                for (String name : splitColumns) {
                    int index = ListArrUtils.getIndex(rowTitle, name);
                    listKey.add(index);
                }
            }
            // 字段排序
            if (!StringUtils.isNullOrEmpty(Columns)) {
                String[] splitColumns = Columns.split(",");
                for (String name : splitColumns) {
                    int index = ListArrUtils.getIndex(rowTitle, name);
                    listColumns.add(index);
                }
            }

            // integer字段排序
            if (!StringUtils.isNullOrEmpty(integerColumns)) {
                String[] splitIntegerColumns = integerColumns.split(",");
                String[] splitColumns = Columns.split(",");
                for (String name : splitIntegerColumns) {
                    int index = ListArrUtils.getIndex(splitColumns, name);
                    listIntegerColumns.add(index);
                }
            }

            for (int i = 1; i < values.size(); i++) {
                List<Object> row = values.get(i);
                String rowData = "";
                String keyData = "";
                if (listKey.size() > 0) {
                    for (int m = 0; m < listKey.size(); m++) {
                        int n = listKey.get(m);
                        String cell = "";
                        if (row.size() > n && n >= 0) {
                            cell = row.get(n).toString().trim().replace("\r", " ").replace("\n", " ");
                        }
                        keyData = keyData + "'" + cell + "'" + ",";
                    }
                }

                if (listColumns.size() > 0) {
                    for (int m = 0; m < listColumns.size(); m++) {
                        int n = listColumns.get(m);
                        String cell = "";
                        boolean contain = ListArrUtils.isContain(listIntegerColumns, m + "");
                        if (contain) {
                            cell = "0";
                        }
                        if (row.size() > n && n >= 0) {
                            cell = row.get(n).toString().trim().replace("\r", " ").replace("\n", " ");
                            if (contain) {
                                cell = cell.replace(",", "");
                                if (StringUtils.isNullOrEmpty(cell)){
                                    cell = "0";
                                }
                            }

                        }
                        rowData = rowData + "'" + cell + "'" + ",";
                    }
                }

                if (listKey.size() > 0) {
                    String s = "'" + MD5Utils.getMD5Str(keyData) + "'" + "," + rowData.substring(0, rowData.length() - 1);
                    if (!StringUtils.isNullOrEmpty(s.split(",")[1].replace("'", ""))) {
                        System.out.println(s);
                        list.add(s);
                    }
                } else {

                    String s = rowData.substring(0, rowData.length() - 1);
                    if (!StringUtils.isNullOrEmpty(s.split(",")[0].replace("'", ""))) {
                        System.out.println(s);
                        list.add(s);
                    }
                }

            }
        }
        return list;
    }


    public static void main(String[] args) throws GeneralSecurityException, IOException {
        List<String> listLotteryConf = GoogleSheetsUtils.readGoogleSheet("1Xq0XdwI84Di1VwahWLRaxgUALeQFM129iPCcSDyQZhU", "Fake Player Consulting!A2:H", null, "Date,Backend/Client,User Name", null);

    }
}

