/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.master;

/**
 *
 * @author cracker
 */
public class INode {

    protected String name;
    // 该文件对应的INode列表，每一个ChunkInfo里面对应一个chunk的不同备份
    protected ChunkInfo[] chunks;

    public INode() {
        name = null;
        chunks = null;
    }

    public INode(String fileName) {
        name = fileName;
        chunks = null;
    }

    public INode(String name, ChunkInfo[] chunks) {
        this.name = name;
        this.chunks = chunks;
    }

    // 获取该文件对应的chunk列表
    ChunkInfo[] getChunks() {
        return this.chunks;
    }

    void addChunk(ChunkInfo newchunk) {
        if (this.chunks == null) {
            this.chunks = new ChunkInfo[1];
            this.chunks[0] = newchunk;
        } else {
            int size = this.chunks.length;
            ChunkInfo[] newlist = new ChunkInfo[size + 1];
            System.arraycopy(this.chunks, 0, newlist, 0, size);
            newlist[size] = newchunk;
            this.chunks = newlist;
        }
    }

    void setChunk(int index, ChunkInfo chk) {
        this.chunks[index] = chk;
    }

    String getName() {
        return this.name;
    }
}
