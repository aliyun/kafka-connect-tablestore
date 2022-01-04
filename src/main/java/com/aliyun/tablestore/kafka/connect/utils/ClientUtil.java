package com.aliyun.tablestore.kafka.connect.utils;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeNotRetryStrategy;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.model.StsUserBo;
import com.aliyun.tablestore.kafka.connect.service.StsService;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author lihn
 * @Date 2021/12/21 16:18
 */
public class ClientUtil {


    public static ClientConfiguration getClientConfiguration(WriterConfig writerConfig) {
        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(writerConfig.getClientMaxConnections());
        switch (writerConfig.getWriterRetryStrategy()) {
            case CERTAIN_ERROR_CODE_NOT_RETRY:
                cc.setRetryStrategy(new CertainCodeNotRetryStrategy(TableStoreSinkConfig.CLIENT_RETRY_TIME_SECONDS, TimeUnit.SECONDS));
                break;
            case CERTAIN_ERROR_CODE_RETRY:
            default:
                cc.setRetryStrategy(new CertainCodeRetryStrategy(TableStoreSinkConfig.CLIENT_RETRY_TIME_SECONDS, TimeUnit.SECONDS));
        }

        return cc;
    }

    /**
     * 基于用户机器核数，内部构建合适的线程池（N为机器核数）
     * core/max:    支持用户配置，默认：核数+1
     * blockQueue:  支持用户配置，1024
     * Reject:      CallerRunsPolicy
     */
    public static ExecutorService createThreadPool(WriterConfig writerConfig) {
        int coreThreadCount = writerConfig.getCallbackThreadCount();
        int maxThreadCount = coreThreadCount;
        int queueSize = writerConfig.getCallbackThreadPoolQueueSize();

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "writer-callback-" + counter.getAndIncrement());
            }
        };

        return new ThreadPoolExecutor(coreThreadCount, maxThreadCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(queueSize), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static void refreshCredential(String accountId, String regionId, String stsEndpoint, String stsAccessId, String stsAccessKey, String roleName, CredentialsProvider credentialsProvider) {
        StsUserBo stsUserBo = StsService.getAssumeRole(accountId, regionId, stsEndpoint, stsAccessId, stsAccessKey, roleName);
        credentialsProvider.setCredentials(new DefaultCredentials(stsUserBo.getAk(), stsUserBo.getSk(), stsUserBo.getToken()));
    }
}
