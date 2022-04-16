package bigdata.hw4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class KProducer {
    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        Properties kafkaProps=new Properties();
        kafkaProps.put("bootstrap.servers","dicvmd7.ie.cuhk.edu.hk:6667");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        FileInputStream fis=new FileInputStream(new File(args[0]));
        BufferedReader br=new BufferedReader(new InputStreamReader(fis));
        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String line1;
        String line2;
        long time1;
        long time2;
        int gap;
        line1=br.readLine();
        time1=dateFormat.parse(line1.split(",")[1]).getTime();
        KafkaProducer<String,String> producer=new KafkaProducer<>(kafkaProps);
        ProducerRecord<String,String> record=new ProducerRecord<>("1155164941-hw4",line1);
        producer.send(record);
        while ((line2= br.readLine())!=null){
            String[] words=line2.split(",");
            time2=dateFormat.parse(words[words.length-1]).getTime();
            gap=(int) (time2-time1);
            record=new ProducerRecord<>("1155164941-hw4",line2);
            Thread.currentThread().sleep(gap);
            producer.send(record);
            time1=time2;
        }
    }
}
