package com.dtwave.hive;

import org.json.JSONObject;
import org.json.JSONException;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * hive udf: 抽取投诉内容中的位置、对象、问题。
 *
 * for example:
 *
 * 输入: 我家里卫生间的马桶漏水了
 * 输出: {"位置": "卫生间","对象":"马桶","问题":"漏水"}
 *
 * @author baisong
 * @date 18/2/1
 */
public class ExtractComplaintUDF extends UDF {


    /**
     * 抽取投诉内容中的位置、对象、问题(因为是Demo，此处未采用NLP算法。)。
     *
     * @param text 投诉内容
     * @return 抽取后的位置、对象、问题
     */
    public String evaluate(String text) {
        if (text == null || text.isEmpty()) return null;

        switch (text) {
            case "我家里卫生间的马桶漏水了":
                return buildResult("卫生间", "马桶", "漏水");
            case "我家洗手间的门槛石缺损了,请尽快维修":
                return buildResult("洗手间", "门槛石", "缺损");
            case "客服你好,厨房台面有点开裂":
                return buildResult("厨房", "台面", "开裂");
            case "我要投诉,我家的进户门门框开裂严重,很长时间了还不来维修":
                return buildResult("进户门", "门框", "开裂");
            case "厨房窗户关不上":
                return buildResult("厨房", "窗户", "关不上");
            case "你好,我家厨房的地漏堵着了":
                return buildResult("厨房", "地漏", "堵");
            case "卫生间马桶下水慢,请快速解决":
                return buildResult("卫生间", "马桶", "下水慢");
            case "浴室门上都是划痕":
                return buildResult("浴室", "门", "划痕");
            case "卫生间水管渗水,房子都不能住了":
                return buildResult("卫生间", "水管", "渗水");
            case "卫生间门把手松动,门都发不开,快点来维修":
                return buildResult("卫生间", "门把手", "松动");
            case "投诉好久了我家的主卧地板发黑,现在都没人管!":
                return buildResult("主卧", "地板", "发黑");
            case "客服人员,卧室门吸吸不住,很影响使用":
                return buildResult("卧室", "门吸", "吸不住");
            case "你好,卧室门有污渍,清理不掉":
                return buildResult("卧室", "门", "污渍");
            default:
                return buildResult("未知", "未知", "未知");
        }
    }


    /**
     * 构造输出结果
     * @param position 位置
     * @param object   对象
     * @param problem  问题
     * @return ...
     */
    private String buildResult(String position, String object, String problem) {
        JSONObject record = null;
        try {
            record = new JSONObject();
            record.put("position", position);
            record.put("object", object);
            record.put("problem", problem);
        }catch (JSONException e) {
            e.printStackTrace();
        }
        return record.toString();
    }
}
