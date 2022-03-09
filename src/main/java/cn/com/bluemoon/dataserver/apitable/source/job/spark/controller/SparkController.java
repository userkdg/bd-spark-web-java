package cn.com.bluemoon.dataserver.apitable.source.job.spark.controller;

import cn.com.bluemoon.dataserver.apitable.source.job.spark.DataBaseExtractorVo;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.service.ISparkSubmitService;
import cn.com.bluemoon.dataserver.apitable.source.job.spark.vo.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.io.IOException;


@Slf4j
@Controller
public class SparkController {
    @Resource
    private ISparkSubmitService iSparkSubmitService;

    /**
     * 调用service进行远程提交spark任务
     *
     * @param vo 页面参数
     * @return 执行结果
     */
    @ResponseBody
    @PostMapping("/spark/export")
    public Object export(@RequestBody DataBaseExtractorVo vo) {
        try {
            if (StringUtils.isEmpty(vo.getAppName())){
                vo.setAppName("bd-spark-dap-job");
            }
            return iSparkSubmitService.submitApplication(vo.buildSparkApplicationParam(), vo);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            log.error("执行出错：{}", e.getMessage());
            return Result.err(500, e.getMessage());
        }
    }
}
