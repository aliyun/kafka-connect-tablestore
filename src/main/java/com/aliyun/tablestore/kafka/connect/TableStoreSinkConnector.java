package com.aliyun.tablestore.kafka.connect;

import com.aliyun.tablestore.kafka.connect.utils.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableStoreSinkConnector extends SinkConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreSinkConnector.class);

    private TableStoreSinkConfig config;
    private Map<String, String> configProps;

    public TableStoreSinkConnector(){

    }

    /**
     * Connector 初始化
     *
     * @param configProps
     */
    @Override
    public void start(Map<String, String> configProps) {
        this.configProps = configProps;
        this.config = new TableStoreSinkConfig(configProps);

        for (Map.Entry<String, String> entry : configProps.entrySet()) {
            LOGGER.info(entry.getKey() + ": " + entry.getValue());
        }

        LOGGER.info("Load TableStore sink config success!");

    }

    /**
     * 实例化task类
     */
    @Override
    public Class<? extends Task> taskClass() {

        return TableStoreSinkTask.class;
    }

    /**
     * 获取任务配置信息
     *
     * @param maxTasks 最大任务数
     * @return Connector所有任务的配置属性
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(configProps);
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(taskProps);
        }

        return configs;
    }

    /**
     * 停止 Connector
     */
    @Override
    public void stop() {
        LOGGER.info("Shutting down TableStore sink connector!");
    }

    /**
     * 获取 Connector的配置信息
     *
     * @return Connector的配置信息
     */
    @Override
    public ConfigDef config() {
        return TableStoreSinkConfig.CONFIG_DEF;
    }

    /**
     * 获取 Connector的版本
     *
     * @return Connector的版本号
     */
    @Override
    public String version() {
        return Version.getVersion();
    }
}
