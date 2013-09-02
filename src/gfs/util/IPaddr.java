/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 *
 * @author baojingjing
 */
public class IPaddr {

    // 获取本机IP地址
    public static String getIP() throws Exception {
        String ipStr = null;
        Enumeration en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface i = (NetworkInterface) en.nextElement();
            for (Enumeration en2 = i.getInetAddresses(); en2.hasMoreElements();) {
                InetAddress addr = (InetAddress) en2.nextElement();
                if (!addr.isLoopbackAddress()) {
                    if (addr instanceof Inet4Address) {
                        ipStr = addr.toString().substring(1);
                    }
                }
            }
        }
        return ipStr;
    }
}
