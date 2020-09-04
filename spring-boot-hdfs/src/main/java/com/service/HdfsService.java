package com.service;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HdfsService {
    @Value("${hadoop.hdfs.uri}")
    private String hdfsPath;
    @Value("${hadoop.hdfs.user}")
    private String hdfsName;
    private static final int bufferSize = 1024 * 1024 * 64;

    @Autowired
    @Qualifier("conf")
    Configuration conf;

    public FileSystem getFileSystem() {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(hdfsPath),conf,hdfsName);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public boolean existFile(String path){
        boolean isExists = false;
        if(StringUtils.isEmpty(path)){
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        System.out.println("srcPath:"+srcPath);
        try {
            isExists = fs.exists(srcPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isExists;
    }

    public List<Map<String,Object>> readPathInfo(String path){
        System.out.println("12:"+path);
        if(StringUtils.isEmpty(path)){
            return null;
        }
        if (!existFile(path)){
            return null;
        }
        FileSystem fs = getFileSystem();

        Path newPath = new Path(path);
        System.out.println("newPath:"+newPath.getName());
        List<Map<String,Object>> list = new ArrayList<>();
        try {
            FileStatus[] statusList = fs.listStatus(newPath);
            if (statusList != null && statusList.length > 0){
                for (FileStatus fileStatus : statusList){
                    Map<String,Object> map = new HashMap<>();
                    map.put("filePath",fileStatus.getPath());
                    map.put("fileStatus",fileStatus.toString());
                    list.add(map);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public boolean mkdir(String path) {
        if(StringUtils.isEmpty(path)){
            return false;
        }
        if(existFile(path)){
            return true;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isOk = false;
        try {
            isOk = fs.mkdirs(srcPath);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return isOk;

    }

    public void createFile(String path, MultipartFile file){
        try {
            if (StringUtils.isEmpty(path) || file.getBytes() == null){
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String fileName = file.getOriginalFilename();
        FileSystem fs = getFileSystem();
        Path newPath = new Path(path+"/"+fileName);
        try {
            FSDataOutputStream outputStream = fs.create(newPath);
            outputStream.write(file.getBytes());
            outputStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String readFile(String path){
        if (StringUtils.isEmpty(path)){
            return null;
        }
        if(!existFile(path)){
            return null;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        FSDataInputStream inputStream = null;
        StringBuffer sb = null;

        try {
            inputStream = fs.open(srcPath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineTxt = "";
            sb = new StringBuffer();
            while ((lineTxt = reader.readLine()) != null){
                sb.append(lineTxt);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                inputStream.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

    public List<Map<String,String>> listFile(String path){
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }

        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        List<Map<String,String>> returnList = null;
        try {
            RemoteIterator<LocatedFileStatus> filesList = fs.listFiles(srcPath,true);
            returnList = new ArrayList<>();
            while (filesList.hasNext()){
                LocatedFileStatus next = filesList.next();
                String fileName = next.getPath().getName();
                Path filePath = next.getPath();
                Map<String,String> map = new HashMap<>();
                map.put("fileName",fileName);
                map.put("filePath",filePath.toString());
                returnList.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return returnList;
    }

    public boolean renameFile(String oldName, String newName){
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)){
            return false;
        }
        FileSystem fs = getFileSystem();
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isOk = false;
        try {
            isOk = fs.rename(oldPath,newPath);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isOk;
    }

    /**
     * 删除HDFS文件
     * @param path
     * @return
     * @throws Exception
     */
    public boolean deleteFile(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return false;
        }
        if (!existFile(path)) {
            return false;
        }
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(path);
        boolean isOk = fs.deleteOnExit(srcPath);
        fs.close();
        return isOk;
    }

    /**
     * 上传HDFS文件
     * @param path
     * @param uploadPath
     * @throws Exception
     */
    public void uploadFile(String path, String uploadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(uploadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(uploadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyFromLocalFile(false, clientPath, serverPath);
        fs.close();
    }

    /**
     * 下载HDFS文件
     * @param path
     * @param downloadPath
     * @throws Exception
     */
    public void downloadFile(String path, String downloadPath) throws Exception {
        if (StringUtils.isEmpty(path) || StringUtils.isEmpty(downloadPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 上传路径
        Path clientPath = new Path(path);
        // 目标路径
        Path serverPath = new Path(downloadPath);

        // 调用文件系统的文件复制方法，第一个参数是否删除原文件true为删除，默认为false
        fs.copyToLocalFile(false, clientPath, serverPath);
        fs.close();
    }

    /**
     * HDFS文件复制 
     * @param sourcePath
     * @param targetPath
     * @throws Exception
     */
    public void copyFile(String sourcePath, String targetPath) throws Exception {
        if (StringUtils.isEmpty(sourcePath) || StringUtils.isEmpty(targetPath)) {
            return;
        }
        FileSystem fs = getFileSystem();
        // 原始文件路径
        Path oldPath = new Path(sourcePath);
        // 目标路径
        Path newPath = new Path(targetPath);

        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            inputStream = fs.open(oldPath);
            outputStream = fs.create(newPath);

            IOUtils.copyBytes(inputStream, outputStream, bufferSize, false);
        } finally {
            inputStream.close();
            outputStream.close();
            fs.close();
        }
    }

    /**
     * 打开HDFS上的文件并返回byte数组
     * @param path
     * @return
     * @throws Exception
     */
//    public byte[] openFileToBytes(String path) throws Exception {
//        if (StringUtils.isEmpty(path)) {
//            return null;
//        }
//        if (!existFile(path)) {
//            return null;
//        }
//        FileSystem fs = getFileSystem();
//        // 目标路径
//        Path srcPath = new Path(path);
//        try {
//            FSDataInputStream inputStream = fs.open(srcPath);
//            return IOUtils.readFullyToByteArray(inputStream);
//        } finally {
//            fs.close();
//        }
//    }

    /**
     * 打开HDFS上的文件并返回java对象
     * @param path
     * @return
     * @throws Exception
     */
//    public <T extends Object> T openFileToObject(String path, Class<T> clazz) throws Exception {
//        if (StringUtils.isEmpty(path)) {
//            return null;
//        }
//        if (!existFile(path)) {
//            return null;
//        }
//        String jsonStr = readFile(path);
//        return JsonUtil.fromObject(jsonStr, clazz);
//    }

    /**
     * 获取某个文件在HDFS的集群位置
     * @param path
     * @return
     * @throws Exception
     */
    public BlockLocation[] getFileBlockLocations(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        if (!existFile(path)) {
            return null;
        }
        FileSystem fs = getFileSystem();
        // 目标路径
        Path srcPath = new Path(path);
        FileStatus fileStatus = fs.getFileStatus(srcPath);
        return fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
    }

}
