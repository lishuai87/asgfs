/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.protocol;

import gfs.protocol.Chunk;
import java.rmi.Remote;
import java.util.Map;

/**
 *
 * @author ralph、zhujiahao
 */
public interface ChunkServerProtocol extends Remote {

    // 写文件、数据推送
    public int saveChunk(Chunk chunk, byte[] stream, String server) throws Exception;

    public int updateChunk(Chunk chunk, byte[] stream, String socket) throws Exception;
    // 删除chunk

    public int deleteChunk(Chunk chunk) throws Exception;

    public String getUpdate(Chunk chunk) throws Exception;

    // 返回chunk对应的字节流
    public byte[] getChunk(Chunk chunk) throws Exception;

    // 检测chunk数据
    public Map<Long, String> hbCheck() throws Exception;

    // 备份chunk
    public void backupChunk(Chunk chunk, String ip) throws Exception;
}
