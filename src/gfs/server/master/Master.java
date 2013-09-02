/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.master;

import gfs.protocol.Chunk;
import gfs.server.protocol.MasterProtocol;
import gfs.util.IPaddr;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cracker
 */
public class Master extends UnicastRemoteObject implements MasterProtocol {

    String serverIP;
    private FSNamesystem namesystem;
    private Heartbeat hb;

    public Master() throws Exception {
        //serverIP = "127.0.0.1";
        serverIP = IPaddr.getIP();
        namesystem = new FSNamesystem();
        hb = new Heartbeat();
    }

    // 创建线程用于心跳检测
    private class Heartbeat extends Thread {

        Heartbeat() {
            this.start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    sleep(1000 * 60);
                    namesystem.scan();
                }
            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
    }

    @Override
    public void addServer(String ip) throws Exception {
        namesystem.addServer(ip + ":9600");
        System.out.println("Server " + ip + " join in.");
    }

    @Override
    public void addFile(String fileName) throws Exception {
        namesystem.addINode(fileName);
        System.out.println("File " + fileName + " added.");
    }

    @Override
    public List getlastChunk(String fileName) throws Exception {
        return namesystem.getlast(fileName);
    }

    @Override
    public void updateFile(String fileName, long size) throws Exception {
        namesystem.updateINode(fileName, size);
    }

    @Override
    public void deleteFile(String fileName) throws Exception {
        namesystem.deleteINode(fileName);
    }

    @Override
    public List fileList() throws Exception {
        return namesystem.fileList();
    }

    @Override
    public List addChunk(String fileName, int seq, long size, String hash) throws Exception {
        return namesystem.addChunk(fileName, seq, size, hash);
    }

    @Override
    public Map<Chunk, String[]> getChunks(String fileName) throws Exception {
        System.out.println("client request file: " + fileName);
        return namesystem.getChunks(fileName);
    }

    public static void main(String[] argv) throws Exception {
        // 多线程，创建线程用于心跳检测，打印inode列表，chunkserver列表
        Master master = new Master();
        LocateRegistry.createRegistry(9500);

        Naming.rebind("rmi://" + master.serverIP + ":9500/master", master);
        System.out.println("Master IP is " + master.serverIP);
    }
}
