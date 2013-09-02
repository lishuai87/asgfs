/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.protocol;

import java.io.Serializable;

/**
 * chunk是GFS存储文件的基本单元，id由long型标识
 *
 * @author cracker
 */
public class Chunk implements Serializable {

    // 由id、length和时间戳标识一个chunk
    private long chunkId;
    private long numBytes;

    public Chunk(long chkid, long length) {
        this.chunkId = chkid;
        this.numBytes = length;
    }

    public Chunk(Chunk chk) {
        this.chunkId = chk.chunkId;
        this.numBytes = chk.numBytes;
    }

    public long getChunkId() {
        return chunkId;
    }

    public void setChunkId(long chkid) {
        chunkId = chkid;
    }

    public long getNumBytes() {
        return numBytes;
    }

    public void setNumBytes(long len) {
        numBytes = len;
    }

    // 获取chunk对应文件名
    public String getChunkName() {
        return "chk_" + String.valueOf(chunkId);
    }
}
