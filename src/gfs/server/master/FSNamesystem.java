/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.master;

import gfs.protocol.Chunk;
import gfs.server.protocol.ChunkServerProtocol;
import java.rmi.Naming;
import java.util.*;

/**
 *
 * @author cracker
 */
public class FSNamesystem {

    protected List<INode> inodes;
    protected List<String> servers;
    // 每一个server上面对应的chunk
    protected Map<String, List<ChunkInfo>> serverInfo;
    protected static int current = 0;

    public FSNamesystem() {
        inodes = new ArrayList();
        servers = new ArrayList();
        serverInfo = new LinkedHashMap();
    }

    protected synchronized void addINode(String fileName) {
        INode inode = new INode(fileName);
        inodes.add(inode);
    }

    protected List fileList() {
        List<String> files = new ArrayList();
        Iterator iter = inodes.listIterator();
        while (iter.hasNext()) {
            INode inode = (INode) iter.next();
            files.add(inode.name);
        }
        return files;
    }

    protected synchronized List addChunk(String fileName, int seq,
            long length, String hash) {
        Calendar now = Calendar.getInstance();
        Date day = now.getTime();
        int time = (int) day.getTime();
        // 此处按时间戳设置chunk名字，需修改
        String chkName = Integer.toString(time) + "0000"
                + Integer.toString(seq);
        Chunk chunk = new Chunk(Long.parseLong(chkName), length);
        // 获取chunk对应的ChunkInfo，设置对应主机
        ChunkInfo chunkInfo = new ChunkInfo(chunk, seq, hash);
        System.out.println(hash);
        setServers(chunkInfo);
        // 修改inode对应的chunk信息
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                inodes.get(i).addChunk(chunkInfo);
                chunkInfo.setInode(inodes.get(i));
                break;
            }
        }
        List result = new ArrayList();
        result.add(chunk);
        result.add(chunkInfo.getChunkServer());

        return result;
    }

    protected Map<Chunk, String[]> getChunks(String fileName)
            throws Exception {
        Map<Chunk, String[]> chunksMap = new LinkedHashMap();

        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                ChunkInfo[] chunks = inodes.get(i).chunks;
                for (int j = 0; j < chunks.length; j++) {
                    chunksMap.put(chunks[j].chunk,
                            chunks[j].getChunkServer());
                }
                break;
            }
        }
        return chunksMap;
    }

    protected List getlast(String fileName) throws Exception {
        List list = new ArrayList();
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                ChunkInfo[] chunks = inodes.get(i).chunks;
                int j = chunks.length - 1;
                ChunkInfo chunkInfo = chunks[j];
                list.add(chunkInfo.chunk);
                list.add(chunkInfo.getChunkServer());
                list.add(j);
                break;
            }
        }
        return list;
    }

    protected void updateINode(String fileName, long size) throws Exception {
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                ChunkInfo[] chunks = inodes.get(i).chunks;
                int j = chunks.length - 1;

                ChunkInfo chunkInfo = chunks[j];
                chunkInfo.chunk.setNumBytes(size);
                chunkInfo.setNumBytes(size);

                String socket = chunkInfo.getFirst();
                ChunkServerProtocol server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + socket + "/chunkserver");
                String hash = server.getUpdate(chunkInfo.chunk);
                chunkInfo.hash = hash;
                inodes.get(i).chunks[j] = chunkInfo;

                for (int k = 0; k < 2; k++) {
                    if (k == 0) {
                        socket = chunkInfo.getFirst();
                    } else {
                        socket = chunkInfo.getSecond();
                    }

                    List<ChunkInfo> chunklist = serverInfo.get(socket);
                    int index = chunklist.indexOf(chunkInfo);
                    chunklist.set(index, chunkInfo);
                    serverInfo.put(socket, chunklist);
                }
                break;
            }
        }
    }

    // 管理chunkserver
    protected void addServer(String socket) {
        servers.add(socket);
    }

    protected void removeServer(String socket) {
        servers.remove(socket);
    }

    // 分配备份服务器，此处可做负载均衡
    protected void setServers(ChunkInfo chunkInfo) {
        List<ChunkInfo> chunks = new ArrayList();
        if (current == servers.size()) {
            current = 0;
        }
        chunkInfo.setFirst(servers.get(current));
        // 处理serverInfo,对chunkserver添加chunk信息
        if (serverInfo.get(servers.get(current)) != null) {
            chunks = serverInfo.get(servers.get(current));
        }
        chunks.add(chunkInfo);
        serverInfo.put(servers.get(current), chunks);
        current++;
        chunks = new ArrayList();

        if (current == servers.size()) {
            current = 0;
        }
        chunkInfo.setSecond(servers.get(current));
        if (serverInfo.get(servers.get(current)) != null) {
            chunks = serverInfo.get(servers.get(current));
        }
        chunks.add(chunkInfo);
        serverInfo.put(servers.get(current), chunks);
        current++;
    }

    // 用于检错，恢复第二个备份
    protected void setServers(ChunkInfo chunkInfo, String server) {
        List<ChunkInfo> chunks = new ArrayList();
        if (current == servers.indexOf(server)) {
            current++;
        }
        if (current >= servers.size()) {
            current = 0;
        }

        chunkInfo.setSecond(servers.get(current));
        if (serverInfo.get(servers.get(current)) != null) {
            chunks = serverInfo.get(servers.get(current));
        }
        chunks.add(chunkInfo);
        serverInfo.put(servers.get(current), chunks);
        current++;
    }

    protected void deleteINode(String fileName) throws Exception {
        for (int i = 0; i < inodes.size(); i++) {
            if (inodes.get(i).name.equals(fileName)) {
                INode inode = inodes.get(i);
                inodes.remove(i);
                for (int j = 0; j < inode.chunks.length; j++) {
                    for (int k = 0; k < 2; k++) {
                        String socket;
                        if (k == 0) {
                            socket = inode.chunks[j].getFirst();
                        } else {
                            socket = inode.chunks[j].getSecond();
                        }

                        ChunkServerProtocol server = (ChunkServerProtocol) Naming.lookup(
                                "rmi://" + socket + "/chunkserver");
                        server.deleteChunk(inode.chunks[j].chunk);
                        List<ChunkInfo> chunks = serverInfo.get(socket);
                        chunks.remove(inode.chunks[j]);
                        serverInfo.put(socket, chunks);
                    }
                }
                System.out.println("file " + inode.getName() + " removed");
                break;
            }
        }
    }

    // 检测并处理
    protected void scan() throws Exception {
        System.out.println("begin checking...");
        // chunkserver返回数据
        Map<Long, String> chkHash;
        // 出错数据保存
        Map<String, List<ChunkInfo>> failChunk = new LinkedHashMap();
        List<String> failServer = new LinkedList();

        ChunkServerProtocol server;
        // 获取出错chunkserver及 出错chunk
        for (int i = 0; i < servers.size(); i++) {
            List<ChunkInfo> chunks = serverInfo.get(servers.get(i));
            Iterator iter = chunks.iterator();
            try {
                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + servers.get(i) + "/chunkserver");
                chkHash = server.hbCheck();
                // 临时数组，用于添加失效chunk
                List<ChunkInfo> failure = new LinkedList();

                int num = 0;
                while (iter.hasNext()) {
                    ChunkInfo chunkInfo = (ChunkInfo) iter.next();
                    String hash = chkHash.get(chunkInfo.getChunkId());

                    if (hash == null || !hash.equals(chunkInfo.hash)) {
                        System.out.println("chunk " + chunkInfo.getChunkName() + " fault ");
                        // 修改namespace中chunkinfo信息,inode中信息
                        chunkInfo.removeServer(servers.get(i));
                        int idx = inodes.indexOf(chunkInfo.inode);
                        inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);
                        // 修改serverInfo信息
                        serverInfo.get(servers.get(i)).set(num, chunkInfo);

                        failure.add(chunkInfo);
                    }
                    num++;
                }
                // 由于分配chunk都需要通过master，所以遍历一次后，chunkserver不会有剩余的chunk
                failChunk.put(servers.get(i), failure);
            } catch (Exception ex) {
                System.out.println("server " + servers.get(i) + " down!");

                while (iter.hasNext()) {
                    ChunkInfo chunkInfo = (ChunkInfo) iter.next();
                    chunkInfo.removeServer(servers.get(i));

                    int idx = inodes.indexOf(chunkInfo.inode);
                    inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);
                    serverInfo.remove(servers.get(i));
                }
                failServer.add(servers.get(i));
            }
        }

        // 处理出错
        // 1. chunkserver失效
        Iterator iter = failServer.iterator();
        while (iter.hasNext()) {
            String socket = (String) iter.next();
            System.out.println("backup fault server " + socket + " ...");

            List<ChunkInfo> chunks = serverInfo.get(socket);
            Iterator chunkIter = chunks.iterator();
            while (chunkIter.hasNext()) {
                ChunkInfo chunkInfo = (ChunkInfo) chunkIter.next();
                System.out.println("backup chunk " + chunkInfo.getChunkName()
                        + " of fault server " + socket);
                // 节点分配
                setServers(chunkInfo, chunkInfo.getFirst());
                int idx = inodes.indexOf(chunkInfo.inode);
                inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);

                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + chunkInfo.getFirst() + "/chunkserver");
                server.backupChunk(chunkInfo.chunk, chunkInfo.getSecond());
            }
        }

        // 2. chunkserver个别chunk失效，直接复制到该主机上
        Iterator mapIter = failChunk.entrySet().iterator();
        while (mapIter.hasNext()) {
            Map.Entry entry = (Map.Entry) mapIter.next();
            String socket = (String) entry.getKey();
            List<ChunkInfo> chunks = (List) entry.getValue();

            Iterator chunkIter = chunks.iterator();
            while (chunkIter.hasNext()) {
                ChunkInfo chunkInfo = (ChunkInfo) chunkIter.next();
                System.out.println("backup fault chunk "
                        + chunkInfo.getChunkName() + " on server " + socket);
                chunkInfo.setSecond(socket);
                int idx = inodes.indexOf(chunkInfo.inode);
                inodes.get(idx).setChunk(chunkInfo.seq, chunkInfo);

                server = (ChunkServerProtocol) Naming.lookup(
                        "rmi://" + chunkInfo.getFirst() + "/chunkserver");
                server.backupChunk(chunkInfo.chunk, socket);
            }
        }
        System.out.println("check end, next check will after 20m");
    }
}
