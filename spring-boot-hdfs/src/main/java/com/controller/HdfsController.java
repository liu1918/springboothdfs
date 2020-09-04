package com.controller;

import com.entity.Student;
import com.service.HdfsService;
import com.util.Result;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/hdfs")
public class HdfsController {
//    private static Logger LOGGER = LoggerFactory.getLogger(HdfsController.class);

    @Autowired
    HdfsService hdfsService;

    @GetMapping("/readPathInfo")
    public Result readPathInfo(@RequestParam("path") String path) {
        System.out.println("path:" + path);
        List<Map<String, Object>> list = hdfsService.readPathInfo(path);
        return new Result(Result.SUCCESS, "", list);
    }

    /**
     * 创建文件夹
     *
     * @param path
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "mkdir", method = RequestMethod.GET)
    @ResponseBody
    public Result mkdir(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
//            LoggerFactoryR.debug("请求参数为空");
            return new Result(Result.FAILURE, "请求参数为空");
        }
        // 创建空文件夹
        boolean isOk = hdfsService.mkdir(path);
        if (isOk) {
//            LOGGER.debug("文件夹创建成功");
            return new Result(Result.SUCCESS, "文件夹创建成功");
        } else {
//            LOGGER.debug("文件夹创建失败");
            return new Result(Result.FAILURE, "文件夹创建失败");
        }

    }

    /**
     * 获取HDFS文件在集群中的位置
     * @param path
     * @return
     * @throws Exception
     */
    @GetMapping("/getFileBlockLocations")
    public Result getFileBlockLocations(@RequestParam("path") String path) throws Exception {
        BlockLocation[] blockLocations = hdfsService.getFileBlockLocations(path);
        return new Result(Result.SUCCESS, "获取HDFS文件在集群中的位置", blockLocations);
    }

    /**
     * 创建文件
     * @param path
     * @return
     * @throws Exception
     */
    @GetMapping("/createFile")
    public Result createFile(@RequestParam("path") String path, @RequestParam("file") MultipartFile file)
            throws Exception {
        if (StringUtils.isEmpty(path) || null == file.getBytes()) {
            return new Result(Result.FAILURE, "请求参数为空");
        }
        hdfsService.createFile(path, file);
        return new Result(Result.SUCCESS, "创建文件成功");
    }

    /**
     * 读取HDFS文件内容
     * @param path
     * @return
     * @throws Exception
     */
    @GetMapping("/readFile")
    public Result readFile(@RequestParam("path") String path) throws Exception {
        String targetPath = hdfsService.readFile(path);
        return new Result(Result.SUCCESS, "读取HDFS文件内容", targetPath);
    }

//    /**
//     * 读取HDFS文件转换成Byte类型
//     * @param path
//     * @return
//     * @throws Exception
//     */
//    @GetMapping("/openFileToBytes")
//    public Result openFileToBytes(@RequestParam("path") String path) throws Exception {
//        byte[] files = hdfsService.openFileToBytes(path);
//        return new Result(Result.SUCCESS, "读取HDFS文件转换成Byte类型", files);
//    }

//    /**
//     * 读取HDFS文件装换成User对象
//     * @param path
//     * @return
//     * @throws Exception
//     */
//    @GetMapping("/openFileToUser")
//    public Result openFileToUser(@RequestParam("path") String path) throws Exception {
//        Student student = hdfsService.openFileToObject(path, student.class);
//        return new Result(Result.SUCCESS, "读取HDFS文件装换成User对象", user);
//    }

    /**
     * 读取文件列表
     * @param path
     * @return
     * @throws Exception
     */
    @GetMapping("/listFile")
    public Result listFile(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return new Result(Result.FAILURE, "请求参数为空");
        }
        List<Map<String, String>> returnList = hdfsService.listFile(path);
        return new Result(Result.SUCCESS, "读取文件列表成功", returnList);
    }

    /**
     * 重命名文件
     * @param oldName
     * @param newName
     * @return
     * @throws Exception
     * test uri:http://localhost:8088/hdfs/renameFile?oldName=/data/test/word-2.txt&newName=/data/test/word.txt
     */
    @GetMapping("/renameFile")
    public Result renameFile(@RequestParam("oldName") String oldName, @RequestParam("newName") String newName)
            throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return new Result(Result.FAILURE, "请求参数为空");
        }
        boolean isOk = hdfsService.renameFile(oldName, newName);
        if (isOk) {
            return new Result(Result.SUCCESS, "文件重命名成功");
        } else {
            return new Result(Result.FAILURE, "文件重命名失败");
        }
    }

    /**
     * 删除文件
     * @param path
     * @return
     * @throws Exception
     */
    @GetMapping("/deleteFile")
    public Result deleteFile(@RequestParam("path") String path) throws Exception {
        boolean isOk = hdfsService.deleteFile(path);
        if (isOk) {
            return new Result(Result.SUCCESS, "delete file success");
        } else {
            return new Result(Result.FAILURE, "delete file fail");
        }
    }

    /**
     * 上传文件
     * @param path
     * @param uploadPath
     * @return
     * @throws Exception
     * test uri:http://localhost:8088/hdfs/uploadFile?path=d://age.txt&uploadPath=/data/test/age.txt
     */
    @GetMapping("/uploadFile")
    public Result uploadFile(@RequestParam("path") String path, @RequestParam("uploadPath") String uploadPath)
            throws Exception {
        hdfsService.uploadFile(path, uploadPath);
        return new Result(Result.SUCCESS, "upload file success");
    }

    /**
     * 下载文件
     * @param path
     * @param downloadPath
     * @return
     * @throws Exception
     * test uri:http://localhost:8088/hdfs/downloadFile?path=/data/test/word.txt&downloadPath=d://word.txt
     */
    @GetMapping("/downloadFile")
    public Result downloadFile(@RequestParam("path") String path, @RequestParam("downloadPath") String downloadPath)
            throws Exception {
        hdfsService.downloadFile(path, downloadPath);
        return new Result(Result.SUCCESS, "download file success");
    }

    /**
     * HDFS文件复制
     * @param sourcePath
     * @param targetPath
     * @return
     * @throws Exception
     */
    @GetMapping("/copyFile")
    public Result copyFile(@RequestParam("sourcePath") String sourcePath, @RequestParam("targetPath") String targetPath)
            throws Exception {
        hdfsService.copyFile(sourcePath, targetPath);
        return new Result(Result.SUCCESS, "copy file success");
    }

    /**
     * 查看文件是否已存在
     * @param path
     * @return
     * @throws Exception
     */
    @GetMapping("/existFile")
    public Result existFile(@RequestParam("path") String path) throws Exception {
        boolean isExist = hdfsService.existFile(path);
        return new Result(Result.SUCCESS, "file isExist: " + isExist);
    }


}
