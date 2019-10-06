package com.insight;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import scala.collection.mutable.ListBuffer;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class flinkproccess {
    public static void main(String[] args) throws Exception {
        // create execution environment
        System.out.println("executing main");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        System.out.println("got exec env");
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // call HostURLs class to get URLs for all services
        HostURLs urls = new HostURLs();
        System.out.println("got HostURL");
        // create new property of Flink
        // set Zookeeper and Kafka url
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", urls.KAFKA_URL + ":9092");
       // properties.setProperty("zookeeper.connect", urls.ZOOKEEPER_URL + ":2181");
        properties.setProperty("group.id", "flink_consumer");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("ecg-topic",
                new SimpleStringSchema(), properties);
        //kafkaConsumer.setStartFromEarliest();
        System.out.println("got consumer");
        DataStream<String> rawInputStream = env.addSource(kafkaConsumer);
//mgh003,2019-09-26 19:12:43.678160-04:00,-0.10085470085470085721546240620228,0.04576271186440677984919034315681,93.54304635761589281628403114154935


        SingleOutputStreamOperator<List<Double>> windowedoutput = rawInputStream
                .flatMap(new Splitter())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new AggregateFunction<Tuple2<String, Double>, List<Double>, List<Double>> (){
                    /**
                     * Creates a new accumulator, starting a new aggregate.
                     *
                     * <p>The new accumulator is typically meaningless unless a value is added
                     * via .
                     *
                     * <p>The accumulator is the state of a running aggregation. When a program has multiple
                     * aggregates in progress (such as per key and window), the state (per key and window)
                     * is the size of the accumulator.
                     *
                     * @return A new accumulator, corresponding to an empty aggregate.
                     */
                    @Override
                    public List<Double> createAccumulator() {
                        return new ArrayList<Double>();
                    }

                    /**
                     * Adds the given input value to the given accumulator, returning the
                     * new accumulator value.
                     *
                     * <p>For efficiency, the input accumulator may be modified and returned.
                     *
                     * @param value       The value to add
                     * @param accumulator The accumulator to add the value to
                     */
                    @Override
                    public List<Double> add(Tuple2<String, Double> value, List<Double> accumulator) {
                        accumulator.add(value.f1);
                        System.out.println(accumulator);
                        return accumulator;
                    }

                    /**
                     * Gets the result of the aggregation from the accumulator.
                     *
                     * @param accumulator The accumulator of the aggregation
                     * @return The final aggregation result.
                     */
                    @Override
                    public List<Double> getResult(List<Double> accumulator) {
                        return accumulator;
                    }

                    /**
                     * Merges two accumulators, returning an accumulator with the merged state.
                     *
                     * <p>This function may reuse any of the given accumulators as the target for the merge
                     * and return that. The assumption is that the given accumulators will not be used any
                     * more after having been passed to this function.
                     *
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return The accumulator with the merged state
                     */
                    @Override
                    public List<Double> merge(List<Double> a, List<Double> b) {
                        a.addAll(b);
                        return a;
                    }

                    /**
                     * Merges two accumulators, returning an accumulator with the merged state.
                     *
                     * <p>This function may reuse any of the given accumulators as the target for the merge
                     * and return that. The assumption is that the given accumulators will not be used any
                     * more after having been passed to this function.
                     *
                     * @param a An accumulator to merge
                     * @param b Another accumulator to merge
                     * @return The accumulator with the merged state
                     */
                }); // AGGREGATE FUNCTION

        windowedoutput.print();
        //windowedoutput.writeAsCsv("~/Downloads");
        env.execute("Flink process");
    } //MAIN Function


        public static class Splitter implements FlatMapFunction<String, Tuple2<String, Double>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Double>> out) throws Exception {
            List<String> list = new ArrayList<>(Arrays.asList(sentence.split(",")));
            System.out.println(list);
            out.collect(new Tuple2<String, Double>(list.get(0), Double.parseDouble(list.get(1))));
            }
        }//SPLITTER
}//CLASS FUnction
