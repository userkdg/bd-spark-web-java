package cn.com.bluemoon.dataserver.apitable.source.job.spark.service;

import cn.com.bluemoon.dataserver.apitable.source.job.spark.DataBaseExtractorVo;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.SparkApplicationParam;

import java.io.IOException;

/**
 * spark任务提交service
 **/
public interface ISparkSubmitService {

    /**
     * 提交spark任务入口
     *
     * @param sparkAppParams spark任务运行所需参数
     * @param otherParams    单独的job所需参数
     * @return 结果
     * @throws IOException          io
     * @throws InterruptedException 线程等待中断异常
     */
    boolean submitApplication(SparkApplicationParam sparkAppParams, String... otherParams) throws IOException, InterruptedException;

    /**
     * 提交spark任务入口
     *
     * @param sparkAppParams spark任务运行所需参数
     * @param extractorVo    单独的job所需参数
     * @return 结果
     * @throws IOException          io
     * @throws InterruptedException 线程等待中断异常
     */
    boolean submitApplication(SparkApplicationParam sparkAppParams, DataBaseExtractorVo extractorVo) throws IOException, InterruptedException;
}
