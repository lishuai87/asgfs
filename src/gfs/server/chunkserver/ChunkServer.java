/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.chunkserver;

import gfs.protocol.Chunk;
import gfs.server.protocol.ChunkServerProtocol;
import gfs.server.protocol.MasterProtocol;
import gfs.util.IPaddr;
import gfs.util.Security;
import java.io.*;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author ralph、zhujiahao
 */
public class ChunkServer extends UnicastRemoteObject implements ChunkServerProtocol {

    String serverIP;
    List<Long> chunks;
    Map<Long, String> chunkHash;
    MasterProtocol master;
    ChunkServerProtocol server;
    String path = "data/chunkserver/";

    public ChunkServer() throws Exception {
        serverIP = IPaddr.getIP();
        chunks = new ArrayList();
        chunkHash = new ConcurrentHashMap();

        master = (MasterProtocol) Naming.lookup("rmi://192.168.130.128:9500/master");
        master.addServer(serverIP);
        server = null;
        Chkcheck chkcheck = new Chkcheck();
    }

    public ChunkServer(String socket) throws Exception {
        serverIP = IPaddr.getIP();
        chunks = new ArrayList();
        chunkHash = new ConcurrentHashMap();
        master = (MasterProtocol) Naming.lookup("rmi://" + socket + ":9500/master");
        master.addServer(serverIP);
        Chkcheck chkcheck = new Chkcheck();
    }

    // 检测chunk文件hash
    private class Chkcheck extends Thread {

        Chkcheck() {
            this.start();
        }

        @Override
        public void run() {
            try {
                while (true) {
                    sleep(1000 * 60);
                    System.out.println("checking...");

                    Iterator chunkIter = chunks.iterator();
                    while (chunkIter.hasNext()) {
                        Long chunkid = (Long) chunkIter.next();
                        String hash = Security.getMD5sum(path + getChunkName(chunkid));
                        if (hash == null) {
                            hash = "this is unreasonable";
                        }
                        chunkHash.put(chunkid, hash);
                    }
                    System.out.println("check end.");
                }
            } catch (Exception ex) {
                System.out.println(ex);
            }
        }
    }

    private String getChunkName(Long id) {
        return "chk_" + Long.toString(id);
    }

    // 将chunk对应字节流保存为文件
    public synchronized String saveFile(Chunk chk, byte[] data) throws Exception {
        String fileName = chk.getChunkName();
        File file = new File(path + fileName);
        OutputStream output = new FileOutputStream(file, true);
        output.write(data);
        output.close();

        String hash = Security.getMD5sum(path + fileName);
        return hash;
    }

    public void removeFile(Chunk chk) throws Exception {
        String fileName = chk.getChunkName();
        File file = new File(path + fileName);
        file.delete();
    }

    // 获取chunk对应字节流
    public byte[] readFile(Chunk chk) throws IOException {
        String fileName = chk.getChunkName();
        int length = (int) chk.getNumBytes();

        int n, len = 0;
        byte[] buffer = new byte[length];
        byte[] upbytes = new byte[length];

        InputStream input = new FileInputStream(path + fileName);
        while ((n = input.read(buffer, 0, length)) > 0) {
            System.arraycopy(buffer, 0, upbytes, len, n);
            len += n;
        }
        input.close();

        return upbytes;
    }

    @Override
    public int saveChunk(Chunk chunk, byte[] stream, String socket) throws Exception {
        if (deleteChunk(chunk) == 1) {
            System.out.println("chunk " + chunk.getChunkName() + "fail, recovering...");
        }

        // 存储chunk
        String hash = saveFile(chunk, stream);
        chunks.add(chunk.getChunkId());
        chunkHash.put(chunk.getChunkId(), hash);
        System.out.println("chunk " + chunk.getChunkName() + " added.");
        // 数据推送
        if (socket != null) {
            server = (ChunkServerProtocol) Naming.lookup("rmi://" + socket + "/chunkserver");
            int i = 0;
            do {
                i = server.saveChunk(chunk, stream, null);
            } while (i == 0);
        }
        return 1;
    }

    @Override
    public int updateChunk(Chunk chunk, byte[] stream, String socket) throws Exception {
        // 存储chunk
        String hash = saveFile(chunk, stream);
        //chunks.add(chunk.getChunkId());
        chunkHash.put(chunk.getChunkId(), hash);
        System.out.println("chunk " + chunk.getChunkName() + " updated.");
        // 数据推送
        if (socket != null) {
            server = (ChunkServerProtocol) Naming.lookup("rmi://" + socket + "/chunkserver");
            int i = 0;
            do {
                i = server.updateChunk(chunk, stream, null);
            } while (i == 0);
        }
        return 1;
    }

    @Override
    public int deleteChunk(Chunk chunk) throws Exception {
        if (chunks.contains(chunk.getChunkId())) {
            removeFile(chunk);
            chunks.remove(chunk.getChunkId());
            chunkHash.remove(chunk.getChunkId());
            System.out.println("chunk " + chunk.getChunkName() + " delete");
            return 1;
        }
        return 0;
    }

    @Override
    public String getUpdate(Chunk chunk) throws Exception {
        return chunkHash.get(chunk.getChunkId());
    }

    @Override
    public byte[] getChunk(Chunk chunk) throws Exception {
        return readFile(chunk);
    }

    @Override
    public void backupChunk(Chunk chunk, String socket) throws Exception {
        // 存储chunk
        ChunkServerProtocol backupServer = (ChunkServerProtocol) Naming.lookup(
                "rmi://" + socket + "/chunkserver");

        backupServer.saveChunk(chunk, readFile(chunk), null);
    }

    @Override
    public Map<Long, String> hbCheck() throws Exception {
        return chunkHash;
    }

    public static void main(String[] argv) throws Exception {
        ChunkServer server;

        System.out.print("输入master ip: ");
        BufferedReader ipBuffer = new BufferedReader(new InputStreamReader(System.in));
        String masterIP = ipBuffer.readLine();

        if (masterIP.trim().length() != 0) {
            server = new ChunkServer(masterIP);
        } else {
            server = new ChunkServer();
        }

        LocateRegistry.createRegistry(9600);
        Naming.rebind("rmi://" + server.serverIP + ":9600/chunkserver", server);
    }
}