package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;

//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
//import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.json.JSONObject;
//import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
//import com.datastax.driver.core.Cluster;
//import com.typesafe.config.ConfigFactory;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


//import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;


public class WikipediaAnalysis {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "ec2-52-203-199-182.compute-1.amazonaws.com:9092");
        // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "ec2-52-203-199-182.compute-1.amazonaws.com:2181");
    properties.setProperty("group.id", "test");
        //DataStream<String> dataStream = env
        DataStream<Tuple3<String, Long, Integer>> datain = env
	        .addSource(new FlinkKafkaConsumer09<>("jtest", new SimpleStringSchema(), properties))
                .flatMap(new LineSplitter());

        DataStream<Tuple3<String, Long, Integer>> clickcount = datain
            .keyBy(0)
            .timeWindow(Time.seconds(1))
            .sum(2);

	clickcount.print();
	env.execute("Window WordCount");
  }

    public static class LineSplitter implements FlatMapFunction<String, Tuple3<String, Long, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, Long, Integer>> out) {
            String[] word = line.split(";");
            out.collect(new Tuple3<String, Long, Integer>(word[0], Long.parseLong(word[1]), 1));
        }
    }

}
