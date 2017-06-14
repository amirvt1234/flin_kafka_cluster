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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
//////////
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
//import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.common.functions.ReduceFunction;
//////////
*/

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
//import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
//import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class WikipediaAnalysis {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "ec2-52-203-199-182.compute-1.amazonaws.com:9092");
        // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "ec2-52-203-199-182.compute-1.amazonaws.com:2181");
    properties.setProperty("group.id", "test");
        //DataStream<String> dataStream = env
    DataStream<Tuple4<String, Long, Long, Integer>> datain = env
        .addSource(new FlinkKafkaConsumer09<>("jtest", new SimpleStringSchema(), properties))
        .flatMap(new LineSplitter());

    DataStream<Tuple4<String, Long, Long, Integer>> clickcount = datain
        .keyBy(0)
        .timeWindow(Time.seconds(1))
        .sum(3);

//	DataStream<Tuple4<String, Long, Long, Integer>> usersession = datain;
//				.window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
//                .reduce (new ReduceFunction<Tuple4<String, Long, Long, Integer>>() {
//                    public Tuple4<String, Long, Long, Integer> reduce(Tuple4<String, Long, Long, Integer> value1, Tuple4<String, Long, Long, Integer> value2) throws Exception {
//                        return new Tuple4<String, Long, Long, Integer>(value1.f0, value1.f1, value2.f1+10L, value1.f3+value2.f3);
//                    }
//                });

	clickcount.print();
	env.execute("Window WordCount");
  }

    public static class LineSplitter implements FlatMapFunction<String, Tuple4<String, Long, Long, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple4<String, Long, Long, Integer>> out) {
            String[] word = line.split(";");
            out.collect(new Tuple4<String, Long, Long, Integer>(word[0], Long.parseLong(word[1]), Long.parseLong(word[1]), 1));
        }
    }

}
