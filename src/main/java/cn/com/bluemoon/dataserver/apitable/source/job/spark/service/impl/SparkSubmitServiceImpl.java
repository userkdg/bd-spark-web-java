package cn.com.bluemoon.dataserver.apitable.source.job.spark.service.impl;

import cn.com.bluemoon.dataserver.apitable.source.job.spark.DataBaseExtractorVo;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.SparkApplicationParam;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.SparkDataBaseExtractorSwapper;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.service.ISparkSubmitService;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.util.HttpUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.apache.spark.launcher.SparkLauncher.*;


@Service
public class SparkSubmitServiceImpl implements ISparkSubmitService {

    private static final Logger log = LoggerFactory.getLogger(SparkSubmitServiceImpl.class);

    @Value("${spark.driver.name:192.168.235.12}")
    private String driverName;

    @Override
    public boolean submitApplication(SparkApplicationParam sparkAppParams, String... otherParams) throws IOException, InterruptedException {
        log.info("spark任务传入参数：{}", sparkAppParams.toString());
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Map<String, String> confParams = sparkAppParams.getOtherConfParams();
        SparkLauncher launcher = new SparkLauncher()
                .setAppResource(sparkAppParams.getJarPath())
                .setMainClass(sparkAppParams.getMainClass())
                .setMaster(sparkAppParams.getMaster())
                .setDeployMode(sparkAppParams.getDeployMode())
                .setConf(DRIVER_MEMORY, sparkAppParams.getDriverMemory())
                .setConf(EXECUTOR_MEMORY, sparkAppParams.getExecutorMemory())
                .setConf(EXECUTOR_CORES, sparkAppParams.getExecutorCores());
        if (confParams != null && confParams.size() != 0) {
            log.info("开始设置spark job运行参数:{}", JSONObject.toJSONString(confParams));
            for (Map.Entry<String, String> conf : confParams.entrySet()) {
                log.info("{}:{}", conf.getKey(), conf.getValue());
                launcher.setConf(conf.getKey(), conf.getValue());
            }
        }
        if (otherParams != null && otherParams.length > 0) {
            log.info("开始设置spark job参数:{}", Arrays.toString(otherParams));
            launcher.addAppArgs(otherParams);
        }
        log.info("参数设置完成，开始提交spark任务");
        SparkAppHandle handle = launcher.setVerbose(true).startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                log.info("stateChanged:{}", sparkAppHandle.getState().toString());
                if (sparkAppHandle.getState().isFinal()) {
                    countDownLatch.countDown();
                }
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                log.info("infoChanged:{}", sparkAppHandle.getState().toString());
            }
        });
        log.info("appId={},The task is executing, please wait ....", handle.getAppId());
        //线程等待任务结束
        countDownLatch.await();
        log.info("appId={}, The task is finished!", handle.getAppId());
        //通过Spark原生的监测api获取执行结果信息，需要在spark-default.xml、spark-env.sh、yarn-site.xml进行相应的配置
        String restUrl;
        try {
            restUrl = "http://" + driverName + ":18080/api/v1/applications/" + handle.getAppId();
            log.info("访问application运算结果，url:{}", restUrl);
            String historyLog = HttpUtil.httpGet(restUrl, null);
            log.info("sparkHistoryLog: \n{}", historyLog);
        } catch (Exception e) {
            log.warn("18080端口异常，请确保spark-history-server服务已开启");
        }
        return !handle.getState().equals(SparkAppHandle.State.FAILED);
    }

    @Override
    public boolean submitApplication(SparkApplicationParam sparkAppParams, DataBaseExtractorVo extractorVo) throws IOException, InterruptedException {
        return submitApplication(sparkAppParams, SparkDataBaseExtractorSwapper.toJsonStr(extractorVo));
    }
}
