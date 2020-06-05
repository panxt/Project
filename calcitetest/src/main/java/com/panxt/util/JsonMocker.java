package com.panxt.util;

/**
 * 模拟一些电厂数据
 *
 * ES时间格式 : 2020-02-17T07:09:41.048Z
 *
 * {"type","dector","id":1,"device":"192.168.0.1","src_ip":"55.55.55.55","dst_ip":"22.22.22.22","@timestamp":"2020 ","info":"XXX"}
 *
 */
public class JsonMocker {

    int dataNum = 1000;

    //随机电厂数据
    RanData[] facRanDatas = {new RanData("大风电厂", 20),
            new RanData("北风电厂", 30),
            new RanData("燃火电厂", 40),
            new RanData("土升电厂",10)
    };

    RanOptionGroup<String> facOptGroup = new RanOptionGroup<>(facRanDatas);



    //随机IP数据


}
