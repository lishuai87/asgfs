/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs;

import gfs.protocol.Chunk;
import gfs.server.protocol.ChunkServerProtocol;
import gfs.server.protocol.MasterProtocol;
import gfs.util.Security;
import java.io.*;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author ralph
 */
public class DFS {

    // 默认chunk大小
    private final static int defSize = 1024 * 1024 * 7;
    private MasterProtocol master;
    private ChunkServerProtocol server;
    private String path = "data/client/";

    // 初始化远程对象
    public DFS(String socket) throws Exception {
        master = (MasterProtocol) Naming.lookup("rmi://" + socket + "/master");
        server = null;
    }

    // 上传文件
    public void upload(String fileName) throws Exception {
        // 添加inode
        master.addFile(fileName);

        String hash;
        int n, seq = 0;
        byte[] buffer = new byte[defSize];

        InputStream input = new FileInputStream(path + fileName);
        input.skip(0);
        while ((n = input.read(buffer, 0, defSize)) > 0) {
            byte[] upbytes = new byte[n];
            System.arraycopy(buffer, 0, upbytes, 0, n);
            hash = Security.getMD5sum(upbytes);
            uploadChunk(fileName, upbytes, seq, n, hash);
            seq++;
        }
        input.close();
    }

    public void delete(String fileName) throws Exception {
        master.deleteFile(fileName);
    }

    // 上传每一个块
    public void uploadChunk(String fileName, byte[] stream, int seq, long size, String hash)
            throws Exception {
        // 获取chunk对应的chunkserver
        List list = (ArrayList) master.addChunk(fileName, seq, size, hash);
        Chunk chunk = (Chunk) list.get(0);
        String[] sockets = (String[]) list.get(1);

        server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
        server.saveChunk(chunk, stream, sockets[1]); //服务器列表用于数据推送
    }

    public void updateFile(String fileName, byte[] stream) throws Exception {
        List list = master.getlastChunk(fileName);
        Chunk chunk = (Chunk) list.get(0);
        String[] sockets = (String[]) list.get(1);
        int seq = (Integer) list.get(2);
        seq++;

        int ptr = 0;
        int lastleng = (int) chunk.getNumBytes();
        int streamleng = stream.length;

        ptr = defSize - lastleng;
        if (ptr >= streamleng) {
            byte[] upbytes = new byte[streamleng];
            System.arraycopy(stream, 0, upbytes, 0, streamleng);
            server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
            server.updateChunk(chunk, upbytes, sockets[1]); //服务器列表用于数据推送
            master.updateFile(fileName, lastleng + streamleng);
        } else {
            byte[] upbytes = new byte[ptr];
            System.arraycopy(stream, 0, upbytes, 0, ptr);
            String hash = Security.getMD5sum(upbytes);
            server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
            server.updateChunk(chunk, upbytes, sockets[1]); //服务器列表用于数据推送
            //uploadChunk(fileName, upbytes, seq, ptr, hash); 
            master.updateFile(fileName, defSize);
            while (ptr + defSize <= streamleng) {
                upbytes = new byte[defSize];
                System.arraycopy(stream, ptr, upbytes, 0, defSize);
                hash = Security.getMD5sum(upbytes);
                uploadChunk(fileName, upbytes, seq, defSize, hash);
                ptr += defSize;
                seq++;
            }
            if (ptr < streamleng) {
                int lastSize = streamleng - ptr;
                upbytes = new byte[lastSize];
                System.arraycopy(stream, ptr, upbytes, 0, lastSize);
                hash = Security.getMD5sum(upbytes);
                uploadChunk(fileName, upbytes, seq, lastSize, hash);
            }
        }
//        if (lastleng < defSize) {
//            ptr = defSize - lastleng;
//            if (ptr >= streamleng) {
//                byte[] upbytes = new byte[streamleng];
//                System.arraycopy(stream, 0, upbytes, 0, streamleng);
//                server = (ChunkServerProtocol) Naming.lookup("rmi://" + sockets[0] + "/chunkserver");
//                server.saveChunk(chunk, upbytes, sockets[1]); //服务器列表用于数据推送
//                
//                master.updateFile(fileName, lastleng + streamleng);
//            }
//            
//        }
//        while(ptr < streamleng){
//            if(ptr + defSize <= streamleng) {
//                byte[] upbytes = new byte[defSize];
//                System.arraycopy(stream, ptr, upbytes, 0, defSize);
//                String hash = Security.getMD5sum(upbytes);
//                uploadChunk(fileName, upbytes, seq, defSize, hash);
//                
//            } else {
//                int leng = streamleng - ptr;
//                byte[] upbytes = new byte[leng];
//                System.arraycopy(stream, ptr, upbytes, 0, leng);
//                String hash = Security.getMD5sum(upbytes);
//                uploadChunk(fileName, upbytes, seq, leng, hash);
//                master.updateFile(fileName, defSize);
//            }
//            seq++;
//            ptr += defSize;
//        }
    }

    // 获取服务器文件列表
    public List getFileList() throws Exception {
        List list = (ArrayList) master.fileList();
        return list;
    }

    // 下载一个文件
    public void download(String fileName) throws Exception {
        File getFile = new File(path + "new_" + fileName);
        OutputStream output = new FileOutputStream(getFile);

        Map<Chunk, String[]> chunks = (Map) master.getChunks(fileName);

        Iterator iter = chunks.entrySet().iterator();
        int off = 0, length = 0;
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Chunk chunk = (Chunk) entry.getKey();
            String servers[] = (String[]) entry.getValue();
            String socket = servers[0];

            output.write(downloadChunk(chunk, socket));
        }
        output.close();
    }

    public byte[] downloadChunk(Chunk chunk, String socket) throws Exception {
        server = (ChunkServerProtocol) Naming.lookup("rmi://" + socket + "/chunkserver");

        return server.getChunk(chunk);
    }
}