package com.dtwave.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Serializable;
import java.util.Properties;

/**
 * Kafka模拟数据生成
 * <p>
 * 数据格式：
 * {"_id":1,"orderId":18,"proName":"prt3","amount":1,"orderTime":1512974707597}
 * {"_id":2,"orderId":19,"proName":"prt4","amount":1,"orderTime":1512974708601}
 * {"_id":3,"orderId":20,"proName":"prt0","amount":1,"orderTime":1512974709607}
 *
 * @author hulb
 * @date 2017/12/11 下午2:43
 */
public class KafkaDataSimulation {


    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: KafkaDataSimulation <bootstrap.servers>\n" +
                    "Example : mq250:9092,mq221:9092,mq164:9092");
            System.exit(-1);
        }
        Integer messageCount = 10000;
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("request.timeout.ms", 45000);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "snappy");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 1; i <= messageCount; i++) {
            Order order = new Order();
            order.set_id(Integer.toUnsignedLong(i));
            order.setOrderId(Integer.toUnsignedLong(i));
            order.setProName("prt_" + Math.floorMod(i, 5));
            order.setAmount(1);
            long currentTime = System.currentTimeMillis();
            order.setOrderTime(currentTime);
            String adsContent = new ObjectMapper().writeValueAsString(order);
            System.out.println(adsContent);
            ProducerRecord<String, String> adsProduceRecord = new ProducerRecord<>("orders", adsContent);
            producer.send(adsProduceRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.err.println("failed to send the record," + e.getMessage());
                    } else {
                        System.out.println("send successfully");
                    }
                }
            });
            Thread.sleep(1000L);
        }
    }

}

class Order implements Serializable {
    private Long _id;
    private Long orderId;
    private String proName;
    private Integer amount;
    private Long orderTime;

    public Long get_id() {
        return _id;
    }

    public void set_id(Long _id) {
        this._id = _id;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getProName() {
        return proName;
    }

    public void setProName(String proName) {
        this.proName = proName;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public Long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(Long orderTime) {
        this.orderTime = orderTime;
    }

}
