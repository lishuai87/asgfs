/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 *
 * @author baojingjing
 */
public class Security {

    // 获取文件md5
    public static String getMD5sum(String fileName) throws Exception {
        String s = null;
        File file = new File(fileName);
        if (file.exists()) {
            int len = (int) file.length();

            MessageDigest md5 = MessageDigest.getInstance("md5");
            InputStream input = new FileInputStream(file);

            byte[] buffer = new byte[len];
            int length = -1;
            while ((length = input.read(buffer)) != -1) {
                md5.update(buffer, 0, length);
            }
            byte[] code = md5.digest();
            s = byte2hex(code);
            input.close();
        }
        return s;
    }

    public static String getMD5sum(byte[] stream) throws Exception {
        MessageDigest md5 = MessageDigest.getInstance("md5");
        md5.update(stream);
        byte[] code = md5.digest();
        String s = byte2hex(code);
        return s;
    }

    // 二行制转字符串
    protected static String byte2hex(byte[] b) {
        String hs = "";
        String stmp = "";
        for (int n = 0; n < b.length; n++) {
            stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
            if (stmp.length() == 1) {
                hs = hs + "0" + stmp;
            } else {
                hs = hs + stmp;
            }
        }
        return hs.toUpperCase();
    }
}
