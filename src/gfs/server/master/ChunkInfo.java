/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.master;

import gfs.protocol.Chunk;

/**
 * 记录一个chunk所对应的全部备份位置
 *
 * @author cracker
 */
public class ChunkInfo extends Chunk {

    int seq; // 该值在检测时用于修改inode中chunks用到，记录该chunk在文件中的序号
    String hash;
    private String[] doubles;
    Chunk chunk;
    INode inode;

    ChunkInfo(Chunk chk, int seq, String hash) {
        super(chk);
        this.seq = seq;
        this.hash = hash;
        this.doubles = new String[2];
        this.chunk = chk;
    }

    public void setInode(INode inode) {
        this.inode = inode;
    }

    // 设置一个chunk所对应的chunkserver
    public void setChunkServer(String[] triplet) {
        this.doubles = triplet;
    }

    public void removeServer(String str) {
        if (doubles[0].equals(str)) {
            doubles[0] = doubles[1];
        }
        doubles[1] = null;
    }

    public String[] getChunkServer() {
        return this.doubles;
    }

    public void setFirst(String str) {
        this.doubles[0] = str;
    }

    public String getFirst() {
        return this.doubles[0];
    }

    public void setSecond(String str) {
        this.doubles[1] = str;
    }

    public String getSecond() {
        return this.doubles[1];
    }
}
