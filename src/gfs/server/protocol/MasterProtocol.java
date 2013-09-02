/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gfs.server.protocol;

import gfs.protocol.Chunk;
import java.rmi.Remote;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cracker
 */
public interface MasterProtocol extends Remote {

    // 添加文件
    public void addFile(String fileName) throws Exception;

    // 获取文件列表
    public List fileList() throws Exception;

    // 添加chunk
    public List addChunk(String fileName, int seq, long size, String hash) throws Exception;

    // 尾部追加
    public List getlastChunk(String fileName) throws Exception;

    // 删除文件
    public void deleteFile(String fileName) throws Exception;

    public void updateFile(String fileName, long size) throws Exception;

    // 获取文件对应的所有chunk，Map中包括一个chunk所有的备份
    public Map<Chunk, String[]> getChunks(String fileName) throws Exception;

    // chunkserver向master注册
    public void addServer(String ip) throws Exception;
    // chunkserver注销
    //
}
