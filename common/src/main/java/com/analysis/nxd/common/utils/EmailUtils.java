package com.analysis.nxd.common.utils;

import com.analysis.nxd.common.conf.Config;
import io.github.biezhi.ome.OhMyEmail;
import io.github.biezhi.ome.SendMailException;

import static io.github.biezhi.ome.OhMyEmail.SMTP_163;

public class EmailUtils {


    /**
     * @param theme   标题
     * @param context 内容
     */
    public static void sendEmail(String theme, String context) {
        try {
            OhMyEmail.config(SMTP_163(false), "18818406784@163.com", "OXURAULMIHKQFDPZ");
            OhMyEmail.subject(theme)
                    .from("18818406784@163.com")
                    .to("18818406784@163.com")
                    .html(context)
                    .send();
        } catch (SendMailException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        sendEmail("test", "test");
    }
}
