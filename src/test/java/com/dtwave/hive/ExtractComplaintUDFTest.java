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
        System.out.println(demo.evaluate("卫生间门把手松动,门都发不开,快点来维修"));
    }
}