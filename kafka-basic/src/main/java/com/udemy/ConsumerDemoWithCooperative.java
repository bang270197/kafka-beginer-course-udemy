package com.udemy;import org.apache.kafka.clients.consumer.ConsumerRecords;import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;import org.apache.kafka.clients.consumer.KafkaConsumer;import org.apache.kafka.common.errors.WakeupException;import org.slf4j.Logger;import org.slf4j.LoggerFactory;import java.util.Arrays;import java.util.Properties;public class ConsumerDemoWithCooperative {    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithCooperative.class.getName());    public static void main(String[] args) {        logger.info("============ Starting ConsumerDemoWithShutdown ===========");        String groupId = "my-group";        String topic = "TOPIC-2";        Properties props = new Properties();        props.setProperty("bootstrap.servers", "127.0.0.1:9092");        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");        props.put("group.id", groupId);        props.setProperty("auto.offset.reset", "earliest");        //Giảm việc phân bổ lại phân vùng không cần thiết: Bằng cách giữ nguyên các phân vùng đã được phân bổ cho consumer,        // nó tránh được việc phân bổ lại phân vùng quá thường xuyên khi có sự thay đổi consumer.        props.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);        //Lấy ra thread hiện tại mà đoạn mã này đang chạy. Trong trường hợp này, đó là thread chính của chương trình.        final Thread mainThread = Thread.currentThread();        //addShutdownHook()        //Runtime.getRuntime().addShutdownHook(): Đây là cách thêm một "shutdown hook" vào ứng dụng.        //Shutdown hook là một đoạn mã sẽ được thực thi khi ứng dụng nhận được tín hiệu kết thúc, ví dụ như khi nhấn Ctrl+C trong terminal hoặc khi ứng dụng gặp sự cố.        //Nó giúp ứng dụng xử lý các tác vụ dọn dẹp trước khi đóng, chẳng hạn như đóng các kết nối,        // dừng các tiến trình đang chạy hoặc lưu trữ dữ liệu còn lại.        Runtime.getRuntime().addShutdownHook(new Thread(){            public void run() {                logger.info("Deteced a shutdown, let exit by calling cunsumer wake up." );//              //consumer.wakeup(): Đây là cách an toàn để đánh thức Kafka consumer.//              Khi gọi phương thức này, nếu Kafka consumer đang chờ dữ liệu từ Kafka broker thông qua hàm poll(), nó sẽ ném ra một ngoại lệ đặc biệt gọi là WakeupException.//              Điều này giúp thoát khỏi vòng lặp tiêu thụ và kết thúc chương trình một cách có kiểm soát.                //*Lưu ý rằng, wakeup() là non-blocking (không chặn),                // vì vậy nó không dừng ứng dụng ngay lập tức mà chỉ tạo ra tín hiệu "thức dậy".                consumer.wakeup();                try {//                    Tại sao cần mainThread.join(): Khi gọi wakeup(), thread chính sẽ bắt đầu quá trình dừng, nhưng để chắc chắn rằng thread chính hoàn toàn kết thúc trước khi thực thi tiếp các hành động trong shutdown hook, chúng ta cần sử dụng join() để đảm bảo rằng shutdown hook không hoàn thành trước khi thread chính kết thúc.//                            Nói cách khác, chúng ta đợi thread chính kết thúc một cách trật tự trước khi tắt ứng dụng hoàn toàn                    mainThread.join();                    logger.info("Main thread exited.");                } catch (InterruptedException e) {                    e.printStackTrace();                }            }        });        try {            consumer.subscribe(Arrays.asList(topic));            while (true) {                ConsumerRecords<String, String> records = consumer.poll(100);                records.forEach(record -> {                    logger.info("Key: " + record.key() + ", Value: " + record.value());                    logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());                });            }        } catch (WakeupException e) {            logger.info("Consumer is starting to shut down");        } catch (Exception e){            logger.info("Caught Exception", e);        } finally {            consumer.close();            logger.info("The consumer has been shut down");        }    }}