package com.config;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class HdfsConfig {

    @Value("${hadoop.hdfs.uri}")
    private String uri;

    @Bean(name = "conf")
    public Configuration getConf(){
        Configuration conf = new Configuration();
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("fs.trash.interval", "360");

        conf.set("fs.defaultFS", uri);
        return conf;
    }
}
