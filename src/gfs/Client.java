/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * client具体功能，包括上传、下载，获取master上的文件列表
 *
 * @author zhujiahao baojingjing
 */
public class Client {

    public static void main(String[] argv) throws Exception {
        DFS dfs;

        System.out.print("输入master ip: ");
        BufferedReader readBuffer = new BufferedReader(new InputStreamReader(System.in));
        String masterIP = readBuffer.readLine();

        if (masterIP.trim().length() != 0) {
            dfs = new DFS(masterIP + ":9500");
        } else {
            dfs = new DFS("192.168.130.128:9500");
        }

        System.out.println("选择测试功能：");

        System.out.println("1：上传下载");
        System.out.println("2：尾部追加");
        System.out.println("3：删除文件");

        String readnum;
        while ((readnum = readBuffer.readLine()).trim().length() != 0) {
            int num = Integer.parseInt(readnum);
            String fileName;
            switch (num) {
                case 1:
                    fileName = "en.pdf";
                    dfs.upload(fileName);
                    System.out.println("上传完毕");
                    dfs.download(fileName);
                    System.out.println("下载完毕");

                    break;
                case 2:
                    dfs.upload("test_en.pdf");

                    int n;
                    int defSize = 1024 * 1024 * 100;
                    byte[] buffer = new byte[defSize];
                    byte[] upbytes;
                    String path = "data/client/";
                    InputStream input = new FileInputStream(path + "test_en.append");
                    input.skip(0);
                    n = input.read(buffer, 0, defSize);
                    upbytes = new byte[n];
                    System.arraycopy(buffer, 0, upbytes, 0, n);

                    input.close();
                    dfs.updateFile("test_en.pdf", upbytes);
                    dfs.download("test_en.pdf");
                    System.out.println("追加完毕");

                    break;
                case 3:
                    fileName = "tiger.pdf";
                    dfs.upload(fileName);
                    System.out.println("上传完毕");
                    dfs.delete(fileName);
                    System.out.println("删除完成");

                    break;
            }
        }
    }
}
