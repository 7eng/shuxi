package com.dtwave.hive;

import org.junit.Test;

/**
 * 描述信息
 *
 * @author baisong
 * @date 18/2/1
 */
public class ExtractComplaintUDFTest {

    private ExtractComplaintUDF demo=new ExtractComplaintUDF();

    @Test
    public void evaluate() throws Exception {
        System.out.println(demo.evaluate("我家里卫生间的马桶漏水了"));
    }
}