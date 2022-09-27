package cn.com.bluemoon.dataserver.apitable.source.job.spark.service.impl;

import cn.com.bluemoon.dataserver.apitable.source.job.spark.DataBaseExtractorVo;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.SparkApplicationParam;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.SparkDataBaseExtractorSwapper;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.service.ISparkSubmitService;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.util.HttpUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.spark.launcher.SparkLauncher.*;


@Service
public class SparkSubmitServiceImpl implements ISparkSubmitService {

    private static final Logger log = LoggerFactory.getLogger(SparkSubmitServiceImpl.class);
    private static final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    @Value("${spark.driver.name:192.168.235.12}")
    private String driverName;
    @Value("${db.url}")
    private String url;
    @Value("${db.username}")
    private String username;
    @Value("${db.password}")
    private String password;
    @Value("${db.driverClassName}")
    private String driverClassName;

    @Override
    public Object submitApplication(SparkApplicationParam sparkAppParams, DataBaseExtractorVo extractorVo, boolean async) throws IOException, InterruptedException {
        return submitApplication(sparkAppParams, extractorVo, async, SparkDataBaseExtractorSwapper.toJsonStr(extractorVo));
    }

    /**
     * main
     */
    private Object submitApplication(SparkApplicationParam sparkAppParams, DataBaseExtractorVo extractorVo, boolean async, String... otherParams) throws IOException, InterruptedException {
        final SparkLauncher launcher = getSparkLauncher(sparkAppParams, otherParams);
        log.info("参数设置完成，开始提交spark任务");
        final String jobBizId = getJobIdByVo(extractorVo);
        Path tempFile = Files.createFile(Paths.get("bd-spark-" + jobBizId + ".log"));
        State submitStatus = State.FAILED;
        String applicationId = null;
        try {
            CountDownLatch countDownLatch = new CountDownLatch(async ? 0 : 1);
            SparkAppHandle handle = launcher.setVerbose(true)
                    .redirectError(tempFile.toFile())
                    .startApplication(new SparkAppHandle.Listener() {
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
            // 提交作业记录
            List<String> logs = new ArrayList<>();
            logs.add("入参：" + extractorVo.toString(true));
            logs.add("appId=" + handle.getAppId() + ",The task is executing, please wait ....");
            asyncSaveJobLog(handle.getAppId(), extractorVo, State.RUNNING.ordinal(), logs);

            //线程等待任务结束
            countDownLatch.await();
            log.info("appId={}, The task is finished!", handle.getAppId());
//            printSparkJobStatus(handle, async);
            try {
                handle.disconnect();
                handle.stop();
            } catch (Exception e) {
                // nothing
            }
            submitStatus = handle.getState();
            applicationId = handle.getAppId();
        }catch (Exception e){
            log.error("提交作业失败！", e);
            submitStatus = State.FAILED;
        }finally {
            List<String> logs = new ArrayList<>();
            try {
                logs = Files.readAllLines(tempFile, StandardCharsets.UTF_8);
            } catch (MalformedInputException e) {
                logs = Files.readAllLines(tempFile, Charset.forName("gbk"));
            } finally {
                Files.deleteIfExists(tempFile);
            }
            if (logs.stream().anyMatch(log -> log.contains("java.sql.SQLException:"))){
                log.error("日志中发现错误，判断为导出失败！");
                submitStatus = State.FAILED;
            }
            asyncSaveJobLog(applicationId, extractorVo, submitStatus.ordinal(), logs);
        }
        return async ?
                applicationId :
                State.FINISHED.equals(submitStatus); // finished 表示成功！
    }

    private void asyncSaveJobLog(String applicationId, DataBaseExtractorVo extractorVo, int submitStatus, List<String> logs) {
        List<String> fLogs = new ArrayList<>();
        if (logs == null || submitStatus == State.FINISHED.ordinal()) {
            fLogs.add("作业导出成功，详情如下：\n");
        }
        if (logs != null) {
            fLogs.addAll(logs);
        }
        singleThreadExecutor.execute(() -> {
            try {
                // mask
                List<String> maskLogs = fLogs.stream().map(row -> {
                    if (row.contains("[ey")) {
                        return ("    ********<mask>**********");
                    } else if (row.startsWith("ey")) {
                        return ("********<mask>**********");
                    } else {
                        return (row);
                    }
                }).collect(Collectors.toList());
                Class.forName(driverClassName);
                try (Connection conn = DriverManager.getConnection(url, username, password)) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "insert into data_security_export_job_log(work_order_code, log_content, job_status, application_id) values (?,?,?,?) " +
                            "on duplicate key update log_content    = values(log_content)," +
                            "                        job_status     = values(job_status)," +
                            "                        application_id = values(application_id)")) {
                        ps.setString(1, getJobIdByVo(extractorVo));
                        ps.setString(2, String.join("\n", maskLogs));
                        ps.setInt(3, submitStatus);
                        ps.setString(4, applicationId);
                        boolean execute = ps.execute();
                        log.info("导出作业日志记录，作业状态：{}，status:{}", submitStatus, execute);
                    }
                }
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            } finally {
                if (logs != null) {
                    for (String log : logs) {
                        System.out.println(log);
                    }
                }
            }
        });

    }

    private String getJobIdByVo(DataBaseExtractorVo extractorVo) {
        return StringUtils.isBlank(extractorVo.getJobBizId()) ? extractorVo.getAppName() : extractorVo.getJobBizId();
    }

    private void printSparkJobStatus(SparkAppHandle handle, boolean async) {
        //通过Spark原生的监测api获取执行结果信息，需要在spark-default.xml、spark-env.sh、yarn-site.xml进行相应的配置
        String restUrl;
        try {
            restUrl = "http://" + driverName + ":18080/api/v1/applications/" + handle.getAppId();
            log.info("访问application运算结果，url:{}", restUrl);
            if (!async) {
                String historyLog = HttpUtil.httpGet(restUrl, null);
                log.info("sparkHistoryLog: \n{}", historyLog);
            }
        } catch (Exception e) {
            log.warn("18080端口异常，请确保spark-history-server服务已开启");
        }
    }

    /**
     * 构建sparkLauncher
     */
    private SparkLauncher getSparkLauncher(SparkApplicationParam sparkAppParams, String[] otherParams) {
        log.info("spark任务传入参数：{}", sparkAppParams.toString());
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
        return launcher;
    }

}
