package com.alibaba.datax.plugin.reader.cspreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.cspreader.CosReaderErrorCode;

import com.alibaba.datax.plugin.reader.cspreader.Key;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.region.Region;

/**
 * Created by mengxin.liumx on 2014/12/8.
 */
public class CosUtil {
    public static COSClient initOssClient(Configuration conf) {

        String region = conf.getString(Key.REGION);
        String domain = conf.getString(Key.DOMAIN);

        //String endpoint = conf.getString(Key.ENDPOINT);
        String accessId = conf.getString(Key.ACCESSID);
        String accessKey = conf.getString(Key.ACCESSKEY);
        /*ClientConfiguration ossConf = new ClientConfiguration();
        ossConf.setSocketTimeout(Constant.SOCKETTIMEOUT);
        
        // .aliyun.com, if you are .aliyun.ga you need config this
        String cname = conf.getString(Key.CNAME);
        if (StringUtils.isNotBlank(cname)) {
            List<String> cnameExcludeList = new ArrayList<String>();
            cnameExcludeList.add(cname);
            ossConf.setCnameExcludeList(cnameExcludeList);
        }*/

        COSCredentials cred = new BasicCOSCredentials(accessId, accessKey);

        SelfDefinedEndpointBuilder selfDefinedEndpointBuilder = new SelfDefinedEndpointBuilder(region, domain);
        ClientConfig clientConfig = new ClientConfig(new Region(region));

        clientConfig.setEndpointBuilder(selfDefinedEndpointBuilder);
        // 这里建议设置使用 https 协议
        // 从 5.6.54 版本开始，默认使用了 https
        clientConfig.setHttpProtocol(HttpProtocol.http);
        // 3 生成 cos 客户端。


        COSClient client = null;
        try {
            //
            // client = new OSSClient(endpoint, accessId, accessKey, ossConf);
            client =  new COSClient(cred, clientConfig);
        } catch (IllegalArgumentException e) {
            throw DataXException.asDataXException(
                    CosReaderErrorCode.ILLEGAL_VALUE, e.getMessage());
        }

        return client;
    }
}
