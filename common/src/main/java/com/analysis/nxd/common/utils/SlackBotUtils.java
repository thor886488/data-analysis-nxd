package com.analysis.nxd.common.utils;

import com.analysis.nxd.common.conf.Config;
import com.slack.api.Slack;
import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.files.FilesUploadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;

public class SlackBotUtils {

    /**
     * Post a message to a channel your app is in using ID and message text
     */
    public static void publishMessage(String channelId, String text) {
        // you can get this instance via ctx.client() in a Bolt app
        MethodsClient client = Slack.getInstance().methods();
        Logger logger = LoggerFactory.getLogger("my-awesome-slack-app");
        try {
            // Call the chat.postMessage method using the built-in WebClient
            ChatPostMessageResponse result = client.chatPostMessage(r -> r
                            // The token you used to initialize your app
                            .token(Config.SLACK_BOT_TOKEN)
                            .channel(channelId)
                            .text(text)
                    // You could also use a blocks[] array to send richer content
            );
            // Print result, which includes information about the message (like TS)
            logger.info("result {}", result);
        } catch (IOException | SlackApiException e) {
            logger.error("error: {}", e.getMessage(), e);
        }
    }

    /**
     * Post a message to a channel your app is in using ID and message text
     */
    public static void publishErrorMessage(String text) {
        // you can get this instance via ctx.client() in a Bolt app
        MethodsClient client = Slack.getInstance().methods();
        Logger logger = LoggerFactory.getLogger("my-awesome-slack-app");
        try {
            // Call the chat.postMessage method using the built-in WebClient
            ChatPostMessageResponse result = client.chatPostMessage(r -> r
                            // The token you used to initialize your app
                            .token(Config.SLACK_BOT_TOKEN)
                            .channel("C03TEN105KL")
                            .text(text)
                    // You could also use a blocks[] array to send richer content
            );
            // Print result, which includes information about the message (like TS)
            logger.info("result {}", result);
        } catch (IOException | SlackApiException e) {
            logger.error("error: {}", e.getMessage(), e);
        }
    }


    public static void publishSiteMessage(String siteCode, String text) {
        if ("YFT".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_YFT, text);
        } else if ("FH4".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_FH4, text);
        } else if ("BM".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_BM, text);
        } else if ("BM2".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_BM2, text);
        } else if ("2HZN".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_2HZN, text);
        } else if ("1HZ".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_1HZ, text);
        } else if ("FH3".equals(siteCode)) {
            publishMessage(Config.SLACK_CHANNEL_FH3, text);
        }
    }

    public static void filesUpload(String channelId, String filepath, String fileName, String comment) {
        MethodsClient client = Slack.getInstance().methods();
        Logger logger = LoggerFactory.getLogger("my-awesome-slack-app");
        try {

            FilesUploadResponse result = client.filesUpload(r -> r
                    // The token you used to initialize your app is stored in the `context` object
                    .token(Config.SLACK_BOT_TOKEN)
                    .channels(Arrays.asList(channelId))
                    .initialComment(comment)
                    .fileData(File2byte(filepath))
                    .filename(fileName)
                    .file(new File(filepath))
            );
            logger.info("result: {}", result);
        } catch (IOException | SlackApiException e) {
            logger.error("error: {}", e.getMessage(), e);
        }
    }

    /**
     * 将文件转换成byte数组
     *
     * @param filePath
     * @return
     */
    public static byte[] File2byte(String filePath) {
        byte[] buffer = null;
        try {
            FileInputStream fis = new FileInputStream(filePath);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    public static void main(String[] args) throws Exception {
        publishErrorMessage("test") ;
    }


}
