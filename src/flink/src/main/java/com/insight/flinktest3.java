package com.insight;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.primitives.Doubles;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


import java.util.*;


public class flinktest3 {
    public static void main(String[] args) throws Exception {
        // create execution environment
        System.out.println("executing main");
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        System.out.println("got exec env");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);

        // call HostURLs class to get URLs for all services
        HostURLs urls = new HostURLs();
        System.out.println("got HostURL");
        // create new property of Flink
        // set Zookeeper and Kafka url
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", urls.KAFKA_URL);
        properties.setProperty("acks", "all");
        //properties.setProperty("linger.ms", "1000");
        //properties.setProperty("zookeeper.connect", urls.ZOOKEEPER_URL + ":2181");
        //properties.setProperty("group.id", "flink_consumer");

        Properties properties2 = new Properties();
        properties2.setProperty("bootstrap.servers", urls.KAFKA_URL);
        properties2.setProperty("acks", "all");
        properties2.setProperty("linger.ms", "1000");
        //properties2.setProperty("zookeeper.connect", urls.ZOOKEEPER_URL + ":2181");
        properties2.setProperty("group.id", "flink_producer");


        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("ecg-topic2",
                new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();

        FlinkKafkaProducer010<Tuple2<String, List<Double>>> myProducer = new FlinkKafkaProducer010<>("test",
                new SerSchema(), properties2);  // serialization schema
        FlinkKafkaProducer010<String> myProducer2 = new FlinkKafkaProducer010<String>("test",
                new SimpleStringSchema(), properties2);  // serialization schema



//        DataStream<Tuple2<String, List<Double>>> text;
//        text = env.fromElements(ecgData.data);

//        if (params.has("input")) {
//            // read the text file from given input path
//            //text = env.readTextFile(params.get("input"));
//            text = env.fromElements(ecgData.sig);
//        } else {
//            System.out.println("Executing WordCount example with default input data set.");
//            System.out.println("Use --input to specify file input.");
//            // get default test text data
//            text = env.fromElements(ecgData.sig);
//        }
        //text.print().setParallelism(1).name("TEXT");

        System.out.println("got consumer");
        DataStream<String> rawInputStream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(new PunctuatedAssigner());
        //rawInputStream.print();
//mgh003,2019-09-26 19:12:43.678160-04:00,-0.10085470085470085721546240620228,0.04576271186440677984919034315681,93.54304635761589281628403114154935
// yyyy-MM-dd HH:mm:ss.SSZ

        DataStream<Tuple2<String, double[]>> output = rawInputStream
                .flatMap(new Splitter()).setParallelism(1)
                .keyBy(t->t.f0)
                .countWindow(1000)
                .aggregate(new SignalAggregate());
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .aggregate(new SignalAggregate());
       //DataStream<Tuple2<String, String>> out2= windowedoutput.process(new MyProcessWindowFunction());
        //out2.print();
       // out2.addSink(myProducer2);
//        output.print();


        //windowedoutput.writeAsText("~/Downloads/output.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        //rawInputStream.addSink(myProducer2).name("flink-producer").setParallelism(1);

          //taStream output2 = windowedoutput.timeWindowAll(Time.milliseconds(1000)).print();
       // DataStream<String, Tuple, TimeWindow> windowedoutput2 = windowedoutput.keyBy(0).max();
//                .flatMap(new findpeaks())
//                .setParallelism(1)
//                .keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)));

//        windowedoutput2.addSink(myProducer2).name("win-topic");
        //text.addSink(myProducer2).name("just text").setParallelism(1);
//        DataStream<String> windowedoutput2 = windowedoutput
//                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
//                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
//                    @Override
//                    public void process(Context context, Iterable<Tuple2<String, List<Double>>> elements, Collector<String> out) throws Exception {
//                        System.out.println("PROCESS FUNCTION");
//                        String s = elements.iterator().next().f0;
//                        out.collect(s);
//                    }
//                }).broadcast();

 //      windowedoutput2.print();
        DataStream<String> out2 = output.flatMap(new findpeaks());
        out2.print().name("FIND PEAKS");


        env.execute("Flink job");
    } //MAIN Function



    static class Splitter implements FlatMapFunction<String, Tuple2<String, Double>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Double>> out) throws Exception {
            List<String> list = new ArrayList<>(Arrays.asList(sentence.split(",")));
           // List<Double> signal = new ArrayList<>();
            Double signal= Double.parseDouble(list.get(2));
            out.collect(new Tuple2<String, Double>(list.get(0), signal));
        }
    }//SPLITTER

//    static class Splitter2 implements FlatMapFunction<String, Tuple3<String, String, Double>> {
//        @Override
//        public void flatMap(String sentence, Collector<Tuple3<String,String, Double>> out) throws Exception {
//            List<String> list = new ArrayList<>(Arrays.asList(sentence.split(",")));
//            // List<Double> signal = new ArrayList<>();
//            Double signal= Double.parseDouble(list.get(2));
//            out.collect(new Tuple3<String, String, Double>(list.get(0),list.get(1), signal));
//        }
//    }//SPLITTER
    static class findpeaks implements FlatMapFunction<Tuple2<String, double[]>, String> {
        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param out   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        @Override
        public void flatMap(Tuple2<String, double[]> value, Collector<String> out) throws Exception {
            System.out.println("IN FIND PEAKS");
           // peak p = new peak();
//            List<Double> records = new ArrayList<>();
//            records = value.f1;
//            //System.out.println(records);
//            double[] ecg_data = new double[records.size()];
//            for (int i = 0; i < records.size(); i++)
//            {
//                ecg_data[i] = records.get(i);
//            }
            double[] ecg_data = value.f1;
            System.out.println("lenght of signal" + ecg_data.length);
            //System.out.println(Arrays.toString(ecg_data));
            LinkedList<Integer> peaks = peak.findPeaks(ecg_data, 150, 0.75);
            System.out.println(peaks);
            //System.out.println(Arrays.toString(peaks.toArray()));
            if(peaks.size()<2){
                out.collect("Not enough signals");
                return;
            }
            double[] ANE = new double[peaks.size()-1];
            double[] interval = new double[ANE.length];
            for(int i=0;i<peaks.size()-1 ;i++) {
                int start = peaks.get(i);
                int end = peaks.get(i + 1);
                double[] slice = Arrays.copyOfRange(ecg_data, start, end + 1);

                double[] NE = new double[slice.length];

                for (int j = 0; j < slice.length - 2; j++) {
                    NE[j] = slice[j + 1] * slice[j + 1] - slice[j] * slice[j + 2];
                }
                ANE[i] = Arrays.stream(NE).sum() / NE.length;
                Double ANE_Val= ANE[i];
                interval[i] = NE.length / 360.0;
                Double RR = interval[i];
                if (ANE[i] < 0.02) {
                    String result = value.f0+ ", ANE= " + ANE_Val.toString() + " RR= " + RR.toString()+ "result= abnormal";
                    System.out.println(result);
                    out.collect(result);
                    break;
                }
                if (interval[i] < 0.4) {
                    String result = value.f0 + ", ANE= " + ANE_Val.toString() + "RR= " + RR.toString() + "result= abnormal";
                    System.out.println(result);
                    out.collect(result);

                    break;
                }
                else {
                    String result = value.f0 + ", ANE= " + ANE_Val.toString() + "RR= " + RR.toString() + "result= NORMAL";
                    System.out.println(result);
                    out.collect(result);
                }
            }
        }
    }//findpeaks
    static class SignalAggregate implements AggregateFunction<Tuple2<String, Double>
            , Tuple2<String, List<Double>>, Tuple2<String, double[]>>{
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
        public Tuple2<String, List<Double>> createAccumulator() {
            List<Double>  temp = new ArrayList<>();
            Tuple2<String, List<Double>> accumulator = new Tuple2<>("", temp);
            return accumulator;
        }

        /**
         * Adds the given input value to the given accumulator, returning the
         * new accumulator value
         *
         * <p>For efficiency, the input accumulator may be modified and returned.
         *
         * @param value       The value to add
         * @param accumulator The accumulator to add the value to
         */
        @Override
        public Tuple2<String, List<Double>> add(Tuple2<String, Double> value, Tuple2<String, List<Double>>  accumulator) {
            //accumulator.setFilename(value.getFilename());
            System.out.println("PRINT ACCU 4");
//            System.out.println(value.f1);
//            System.out.println(accumulator.f0);
            accumulator.f0 = value.f0;
            //System.out.println("initial accumulator value = " + accumulator.f1.toString());

            accumulator.f1.add(value.f1);
            //accumulator = value.f0 + ", "+ accumulator +", "+ value.f1.toString();
            //System.out.println("after accumulator value = " + accumulator.f1.toString());
            //System.out.println("printing accumulator");
            //System.out.println(value.getSignal());
            //System.out.println(accumulator);
            return accumulator;
        }


        /**
         * Gets the result of the aggregation from the accumulator.
         *
         * @param accumulator The accumulator of the aggregation
         * @return The final aggregation result.
         */
        @Override
        public Tuple2<String, double[]>  getResult(Tuple2<String, List<Double>>  accumulator) {
            double[] target = new double[accumulator.f1.size()];
            for (int i = 0; i < target.length; i++) {
                target[i] = accumulator.f1.get(i);                // java 1.5+ style (outboxing)
            }
            Tuple2<String, double[]> result = new Tuple2<>(accumulator.f0, target);
            return result;
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
        public Tuple2<String, List<Double>>  merge(Tuple2<String, List<Double>>  a, Tuple2<String, List<Double>> b) {
             //b = b.substring(5);
            //return a+b;
            a.f1.addAll(b.f1);
            return a;
        }
    }
    static class SignalAggregate2 implements AggregateFunction<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>{
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
        public Tuple2<String, Double> createAccumulator() {
            Tuple2<String, Double> accumulator = new Tuple2<>("", 0.0);
            return accumulator;
        }

        /**
         * Adds the given input value to the given accumulator, returning the
         * new accumulator value
         *
         * <p>For efficiency, the input accumulator may be modified and returned.
         *
         * @param value       The value to add
         * @param accumulator The accumulator to add the value to
         */
        @Override
        public Tuple2<String, Double> add(Tuple2<String, Double> value, Tuple2<String, Double>  accumulator) {
            //accumulator.setFilename(value.getFilename());
            //System.out.println("printing accumulator");
//            System.out.println(value.f1);
//            System.out.println(accumulator.f0);
            accumulator.f1 = accumulator.f1 + value.f1;
            //System.out.println("initial accumulator value = " + accumulator.f1.toString());

            return accumulator;
        }


        /**
         * Gets the result of the aggregation from the accumulator.
         *
         * @param accumulator The accumulator of the aggregation
         * @return The final aggregation result.
         */
        @Override
        public Tuple2<String, Double>getResult(Tuple2<String, Double> accumulator) {
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
        public Tuple2<String, Double> merge(Tuple2<String, Double>  a, Tuple2<String, Double> b) {
            //b = b.substring(5);
            //return a+b;
            a.f1 = a.f1+b.f1;
            return a;
        }
    }
    static class SerSchema implements KeyedSerializationSchema<Tuple2<String,List<Double>>> {

        /**
         * Serializes the key of the incoming element to a byte array
         * This method might return null if no key is available.
         *
         * @param element The incoming element to be serialized
         * @return the key of the element as a byte array
         */
        @Override
        public byte[] serializeKey(Tuple2<String, List<Double>> element) {
            return element.f0.getBytes();
        }

        /**
         * Serializes the value of the incoming element to a byte array.
         *
         * @param element The incoming element to be serialized
         * @return the value of the element as a byte array
         */
        @Override
        public byte[] serializeValue(Tuple2<String, List<Double>> element) {
            return element.f1.toString().getBytes();
        }

        /**
         * Optional method to determine the target topic for the element.
         *
         * @param element Incoming element to determine the target topic from
         * @return null or the target topic
         */
        @Override
        public String getTargetTopic(Tuple2<String, List<Double>> element) {
            return null;
        }

        /**
         * Optional method to determine the target topic for the element.
         *
         * @param element Incoming element to determine the target topic from
         * @return null or the target topic
         */
    }
    public static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<String> {
//        String format = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX";
        String format = "HH:mm:ss.SS";

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            String[] s =element.split(",");
            //System.out.println("Extracttimestamp");
            //System.out.println(s[1]);

            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = null;
            try {
                date = formatter.parse(s[1]);
                //System.out.println(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long mills = date.getTime();
            //System.out.println(mills);


            return mills;
        }

        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the  method.
         *
         * <p>The returned watermark will be emitted only if it is non-null and its timestamp
         * is larger than that of the previously emitted watermark (to preserve the contract of
         * ascending watermarks). If a null value is returned, or the timestamp of the returned
         * watermark is smaller than that of the last emitted one, then no new watermark will
         * be generated.
         *
         * <p>For an example how to use this method, see the documentation of
         * {@link AssignerWithPunctuatedWatermarks this class}.
         *
         * @param lastElement
         * @param extractedTimestamp
         * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            String[] s =lastElement.split(",");
            //System.out.println("check and get watermark");
            //System.out.println(s[1]);

            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = null;
            try {
                date = formatter.parse(s[1]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long mills = date.getTime();
            //System.out.println(mills);
            if(mills < extractedTimestamp){
                return new Watermark(extractedTimestamp);
            }
            else {
                return null;
            }
        }
    }
    public static class PunctuatedAssigner2 implements AssignerWithPunctuatedWatermarks<Tuple3<String, String, Double>> {
        //        String format = "yyyy-MM-dd HH:mm:ss.SSSSSSXXX";
        String format = "HH:mm:ss.SS";

        @Override
        public long extractTimestamp(Tuple3<String, String, Double> element, long previousElementTimestamp) {
            String s =element.f1;
            //System.out.println("Extracttimestamp");
            //System.out.println(s[1]);

            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = null;
            try {
                date = formatter.parse(s);
                //System.out.println(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long mills = date.getTime();
            //System.out.println(mills);


            return mills;
        }

        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the  method.
         *
         * <p>The returned watermark will be emitted only if it is non-null and its timestamp
         * is larger than that of the previously emitted watermark (to preserve the contract of
         * ascending watermarks). If a null value is returned, or the timestamp of the returned
         * watermark is smaller than that of the last emitted one, then no new watermark will
         * be generated.
         *
         * <p>For an example how to use this method, see the documentation of
         * {@link AssignerWithPunctuatedWatermarks this class}.
         *
         * @param lastElement
         * @param extractedTimestamp
         * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<String, String, Double> lastElement, long extractedTimestamp) {
            String s =lastElement.f1;
            //System.out.println("check and get watermark");
            //System.out.println(s[1]);

            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = null;
            try {
                date = formatter.parse(s);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long mills = date.getTime();
            //System.out.println(mills);
            if(mills < extractedTimestamp){
                return new Watermark(extractedTimestamp);
            }
            else {
                return null;
            }
        }
    }
    private static class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String,Double>, Tuple2<String, String>, String, TimeWindow> {

        public void process(String key,
                            Context context,
                            Iterable<Tuple2<String, Double>> signals,
                            Collector<Tuple2<String, String>> out) {
            String average = "";
            average = average + "," + signals.iterator().next().f1.toString();
            System.out.println("process window");
            System.out.println(average);
            out.collect(new Tuple2<String, String>(key, average));
        }
    }



    //    static class ecgMSG {
//        @JsonProperty("filename")
//        String filename;
//        @JsonProperty("signal")
//        List<Double> signal = new ArrayList<Double>();
//        //public String time  = "";
//        //public String classification = "";
//
//        public ecgMSG(String filename, Double signal) {
//            this.filename = filename;
//            this.signal.add(signal);
//        }
//        public ecgMSG(String filename) {
//            this.filename = filename;
//        }
//        public ecgMSG(){
//            this.filename="";
//        }
//        public String getFilename(){
//            return filename;
//        }
//        public void setFilename(String input){
//            this.filename=input;
//        }
//        //public void settime(String input){
////            this.time=input;
////        }
////        //public void setClassification(String input){
////            this.classification=input;
////        }
//        public List<Double> getSignal(){
//            return signal;
//        }
//
//        public Boolean addSignal(Double input) {
//            return signal.add(input);
//            //return signal;
//        }
//        public Boolean addSignal(List<Double> input) {
//            return signal.addAll(input);
//            //return signal;
//        }
//
////        public String getTime() {
////            return this.time;
////            //return signal;
////        }
////        public String classification() {
////            return this.classification;
////            //return signal;
////        }
//
//        public String toString() {
//            return (filename + ", " + signal.toString());
//        }
//    }
//}
//CLASS FUnction

}
class WordCountData {

    public final static String[] WORDS = new String[] {
            "mgh002,19:12:43.67, 1",
            "mgh002,19:12:43.70, 2",
            "mgh002,19:12:43.73, 3",
            "mgh002,19:12:43.76, 4",
            "mgh004,19:12:43.67, 5",
            "mgh004,19:12:43.70, 6",
            "mgh004,19:12:43.73, 7",
            "mgh004,19:12:43.76, 8"
    };
}

class ecgData {
    public final static double[] sig = new double[] {
            0.05, 0.05, 0.05, 0.05, 0.05,0.05,0.05,0.05,0.05,0.035,
            0.035,0.05, 0.065,0.065,
            0.04,
            0.045,
            0.035,
            0.055,
            0.07,
            0.075,
            0.065,
            0.055,
            0.07,
            0.08,
            0.09,
            0.085,
            0.09,
            0.085,
            0.085,
            0.105,
            0.12,
            0.125,
            0.12,
            0.125,
            0.135,
            0.145,
            0.16,
            0.165,
            0.175,
            0.17,
            0.18,
            0.21,
            0.23,
            0.235,
            0.24,
            0.255,
            0.255,
            0.275,
            0.3,
            0.295,
            0.315,
            0.315,
            0.335,
            0.35,
            0.35,
            0.355,
            0.355,
            0.35,
            0.355,
            0.37,
            0.39,
            0.395,
            0.385,
            0.37,
            0.365,
            0.38,
            0.385,
            0.375,
            0.37,
            0.345,
            0.345,
            0.335,
            0.33,
            0.31,
            0.28,
            0.265,
            0.25,
            0.24,
            0.225,
            0.205,
            0.18,
            0.165,
            0.145,
            0.135,
            0.13,
            0.125,
            0.105,
            0.085,
            0.075,
            0.08,
            0.08,
            0.075,
            0.06,
            0.04,
            0.04,
            0.05,
            0.065,
            0.05,
            0.04,
            0.03,
            0.03,
            0.035,
            0.045,
            0.045,
            0.035,
            0.025,
            0.015,
            0.02,
            0.035,
            0.045,
            0.045,
            0.04,
            0.025,
            0.045,
            0.06,
            0.06,
            0.045,
            0.04,
            0.05,
            0.055,
            0.07,
            0.07,
            0.07,
            0.05,
            0.055,
            0.065,
            0.065,
            0.075,
            0.065,
            0.06,
            0.065,
            0.07,
            0.09,
            0.07,
            0.06,
            0.065,
            0.055,
            0.065,
            0.065,
            0.08,
            0.075,
            0.07,
            0.06,
            0.06,
            0.065,
            0.07,
            0.05,
            0.045,
            0.045,
            0.05,
            0.06,
            0.05,
            0.04,
            0.03,
            0.02,
            0.02,
            0.04,
            0.025,
            0.025,
            0.025,
            0.01,
            0.015,
            0.03,
            0.01,
            0.00,
            -0.01,
            -0.01,
            -0.005,
            0.005,
            0.00,
            -0.005,
            -0.02,
            -0.02,
            -0.01,
            -0.01,
            -0.01,
            -0.025,
            -0.035,
            -0.03,
            -0.02,
            -0.015,
            -0.015,
            -0.03,
            -0.045,
            -0.04,
            -0.04,
            -0.02,
            -0.035,
            -0.03,
            -0.045,
            -0.04,
            -0.025,
            -0.015,
            -0.015,
            -0.025,
            -0.035,
            -0.03,
            -0.015,
            -0.005,
            -0.01,
            -0.02,
            -0.03,
            -0.025,
            -0.02,
            -0.01,
            -0.02,
            -0.015,
            -0.035,
            -0.04,
            -0.04,
            -0.04,
            -0.06,
            -0.07,
            -0.085,
            -0.1,
            -0.09,
            -0.055,
            -0.055,
            -0.035,
            -0.035,
            -0.045,
            -0.025,
            -0.03,
            -0.035,
            -0.045,
            -0.07,
            -0.07,
            -0.06,
            -0.05,
            -0.055,
            -0.065,
            -0.075,
            -0.085,
            -0.075,
            -0.065,
            -0.075,
            -0.08,
            -0.08,
            -0.08,
            -0.065,
            -0.06,
            -0.06,
            -0.065,
            -0.06,
            -0.065,
            -0.06,
            -0.05,
            -0.05,
            -0.06,
            -0.075,
            -0.07,
            -0.055,
            -0.055,
            -0.085,
            -0.105,
            -0.11,
            -0.075,
            -0.035,
            0.01,
            0.095,
            0.235,
            0.43,
            0.67,
            0.91,
            1.16,
            1.365,
            1.435,
            1.38,
            1.17,
            0.82,
            0.38,
            -0.03
            , -0.345
            , -0.475
            , -0.48
            , -0.41
            , -0.3
            , -0.195
            , -0.1
            , -0.04
            , -0.02
            , -0.01
            , -0.02
            , -0.035
            , -0.04
            , -0.045
            , -0.05
            , -0.045
            , -0.04
            , -0.055
            , -0.06
            , -0.06
            , -0.075
            , -0.055
            , -0.05
            , -0.05
            , -0.06
            , -0.06
            , -0.06
            , -0.05
            , -0.045
            , -0.055
            , -0.065
            , -0.06
            , -0.055
            , -0.045
            , -0.03
            , -0.045
            , -0.04
            , -0.05
            , -0.05, -0.035, -0.025, -0.02, -0.04, -0.04, -0.04, -0.035, -0.025, -0.02,
            -0.02, -0.02, -0.02, -0.005, 0.005, -0.005, 0.00, -0.005, 0.00, 0.005, 0.035, 0.03, 0.035, 0.025,
            0.03, 0.055, 0.08, 0.06, 0.065, 0.075, 0.08, 0.11, 0.14, 0.135, 0.15, 0.155,
            0.155, 0.18, 0.2, 0.205, 0.21, 0.22, 0.225, 0.24, 0.265, 0.275,
            0.26, 0.27, 0.275, 0.28, 0.3, 0.3, 0.3, 0.3, 0.295,
            0.305, 0.32, 0.295, 0.285, 0.275, 0.26, 0.26, 0.26, 0.245,
            0.22, 0.205, 0.19, 0.17, 0.17, 0.14,
            0.115, 0.1, 0.085, 0.065, 0.07, 0.04, 0.03, 0.01, -0.01, 0.005, 0.05,
            0.05, 0.05, 0.05, 0.05,0.05,0.05,0.05,0.05,0.035,
            0.035,0.05, 0.065,0.065,
            0.04,
            0.045,
            0.035,
            0.055,
            0.07,
            0.075,
            0.065,
            0.055,
            0.07,
            0.08,
            0.09,
            0.085,
            0.09,
            0.085,
            0.085,
            0.105,
            0.12,
            0.125,
            0.12,
            0.125,
            0.135,
            0.145,
            0.16,
            0.165,
            0.175,
            0.17,
            0.18,
            0.21,
            0.23,
            0.235,
            0.24,
            0.255,
            0.255,
            0.275,
            0.3,
            0.295,
            0.315,
            0.315,
            0.335,
            0.35,
            0.35,
            0.355,
            0.355,
            0.35,
            0.355,
            0.37,
            0.39,
            0.395,
            0.385,
            0.37,
            0.365,
            0.38,
            0.385,
            0.375,
            0.37,
            0.345,
            0.345,
            0.335,
            0.33,
            0.31,
            0.28,
            0.265,
            0.25,
            0.24,
            0.225,
            0.205,
            0.18,
            0.165,
            0.145,
            0.135,
            0.13,
            0.125,
            0.105,
            0.085,
            0.075,
            0.08,
            0.08,
            0.075,
            0.06,
            0.04,
            0.04,
            0.05,
            0.065,
            0.05,
            0.04,
            0.03,
            0.03,
            0.035,
            0.045,
            0.045,
            0.035,
            0.025,
            0.015,
            0.02,
            0.035,
            0.045,
            0.045,
            0.04,
            0.025,
            0.045,
            0.06,
            0.06,
            0.045,
            0.04,
            0.05,
            0.055,
            0.07,
            0.07,
            0.07,
            0.05,
            0.055,
            0.065,
            0.065,
            0.075,
            0.065,
            0.06,
            0.065,
            0.07,
            0.09,
            0.07,
            0.06,
            0.065,
            0.055,
            0.065,
            0.065,
            0.08,
            0.075,
            0.07,
            0.06,
            0.06,
            0.065,
            0.07,
            0.05,
            0.045,
            0.045,
            0.05,
            0.06,
            0.05,
            0.04,
            0.03,
            0.02,
            0.02,
            0.04,
            0.025,
            0.025,
            0.025,
            0.01,
            0.015,
            0.03,
            0.01,
            0.00,
            -0.01,
            -0.01,
            -0.005,
            0.005,
            0.00,
            -0.005,
            -0.02,
            -0.02,
            -0.01,
            -0.01,
            -0.01,
            -0.025,
            -0.035,
            -0.03,
            -0.02,
            -0.015,
            -0.015,
            -0.03,
            -0.045,
            -0.04,
            -0.04,
            -0.02,
            -0.035,
            -0.03,
            -0.045,
            -0.04,
            -0.025,
            -0.015,
            -0.015,
            -0.025,
            -0.035,
            -0.03,
            -0.015,
            -0.005,
            -0.01,
            -0.02,
            -0.03,
            -0.025,
            -0.02,
            -0.01,
            -0.02,
            -0.015,
            -0.035,
            -0.04,
            -0.04,
            -0.04,
            -0.06,
            -0.07,
            -0.085,
            -0.1,
            -0.09,
            -0.055,
            -0.055,
            -0.035,
            -0.035,
            -0.045,
            -0.025,
            -0.03,
            -0.035,
            -0.045,
            -0.07,
            -0.07,
            -0.06,
            -0.05,
            -0.055,
            -0.065,
            -0.075,
            -0.085,
            -0.075,
            -0.065,
            -0.075,
            -0.08,
            -0.08,
            -0.08,
            -0.065,
            -0.06,
            -0.06,
            -0.065,
            -0.06,
            -0.065,
            -0.06,
            -0.05,
            -0.05,
            -0.06,
            -0.075,
            -0.07,
            -0.055,
            -0.055,
            -0.085,
            -0.105,
            -0.11,
            -0.075,
            -0.035,
            0.01,
            0.095,
            0.235,
            0.43,
            0.67,
            0.91,
            1.16,
            1.365,
            1.435,
            1.38,
            1.17,
            0.82,
            0.38,
            -0.03
            , -0.345
            , -0.475
            , -0.48
            , -0.41
            , -0.3
            , -0.195
            , -0.1
            , -0.04
            , -0.02
            , -0.01
            , -0.02
            , -0.035
            , -0.04
            , -0.045
            , -0.05
            , -0.045
            , -0.04
            , -0.055
            , -0.06
            , -0.06
            , -0.075
            , -0.055
            , -0.05
            , -0.05
            , -0.06
            , -0.06
            , -0.06
            , -0.05
            , -0.045
            , -0.055
            , -0.065
            , -0.06
            , -0.055
            , -0.045
            , -0.03
            , -0.045
            , -0.04
            , -0.05
            , -0.05, -0.035, -0.025, -0.02, -0.04, -0.04, -0.04, -0.035, -0.025, -0.02,
            -0.02, -0.02, -0.02, -0.005, 0.005, -0.005, 0.00, -0.005, 0.00, 0.005, 0.035, 0.03, 0.035, 0.025,
            0.03, 0.055, 0.08, 0.06, 0.065, 0.075, 0.08, 0.11, 0.14, 0.135, 0.15, 0.155,
            0.155, 0.18, 0.2, 0.205, 0.21, 0.22, 0.225, 0.24, 0.265, 0.275,
            0.26, 0.27, 0.275, 0.28, 0.3, 0.3, 0.3, 0.3, 0.295,
            0.305, 0.32, 0.295, 0.285, 0.275, 0.26, 0.26, 0.26, 0.245,
            0.22, 0.205, 0.19, 0.17, 0.17, 0.14,
            0.115, 0.1, 0.085, 0.065, 0.07, 0.04, 0.03, 0.01, -0.01, 0.005};
    public final static List<Double> list = Doubles.asList(new double[]{0.05, 0.05, 0.05, 0.05, 0.05,0.05,0.05,0.05,0.05,0.035,
            0.035,0.05, 0.065,0.065,
            0.04,
            0.045,
            0.035,
            0.055,
            0.07,
            0.075,
            0.065,
            0.055,
            0.07,
            0.08,
            0.09,
            0.085,
            0.09,
            0.085,
            0.085,
            0.105,
            0.12,
            0.125,
            0.12,
            0.125,
            0.135,
            0.145,
            0.16,
            0.165,
            0.175,
            0.17,
            0.18,
            0.21,
            0.23,
            0.235,
            0.24,
            0.255,
            0.255,
            0.275,
            0.3,
            0.295,
            0.315,
            0.315,
            0.335,
            0.35,
            0.35,
            0.355,
            0.355,
            0.35,
            0.355,
            0.37,
            0.39,
            0.395,
            0.385,
            0.37,
            0.365,
            0.38,
            0.385,
            0.375,
            0.37,
            0.345,
            0.345,
            0.335,
            0.33,
            0.31,
            0.28,
            0.265,
            0.25,
            0.24,
            0.225,
            0.205,
            0.18,
            0.165,
            0.145,
            0.135,
            0.13,
            0.125,
            0.105,
            0.085,
            0.075,
            0.08,
            0.08,
            0.075,
            0.06,
            0.04,
            0.04,
            0.05,
            0.065,
            0.05,
            0.04,
            0.03,
            0.03,
            0.035,
            0.045,
            0.045,
            0.035,
            0.025,
            0.015,
            0.02,
            0.035,
            0.045,
            0.045,
            0.04,
            0.025,
            0.045,
            0.06,
            0.06,
            0.045,
            0.04,
            0.05,
            0.055,
            0.07,
            0.07,
            0.07,
            0.05,
            0.055,
            0.065,
            0.065,
            0.075,
            0.065,
            0.06,
            0.065,
            0.07,
            0.09,
            0.07,
            0.06,
            0.065,
            0.055,
            0.065,
            0.065,
            0.08,
            0.075,
            0.07,
            0.06,
            0.06,
            0.065,
            0.07,
            0.05,
            0.045,
            0.045,
            0.05,
            0.06,
            0.05,
            0.04,
            0.03,
            0.02,
            0.02,
            0.04,
            0.025,
            0.025,
            0.025,
            0.01,
            0.015,
            0.03,
            0.01,
            0.00,
            -0.01,
            -0.01,
            -0.005,
            0.005,
            0.00,
            -0.005,
            -0.02,
            -0.02,
            -0.01,
            -0.01,
            -0.01,
            -0.025,
            -0.035,
            -0.03,
            -0.02,
            -0.015,
            -0.015,
            -0.03,
            -0.045,
            -0.04,
            -0.04,
            -0.02,
            -0.035,
            -0.03,
            -0.045,
            -0.04,
            -0.025,
            -0.015,
            -0.015,
            -0.025,
            -0.035,
            -0.03,
            -0.015,
            -0.005,
            -0.01,
            -0.02,
            -0.03,
            -0.025,
            -0.02,
            -0.01,
            -0.02,
            -0.015,
            -0.035,
            -0.04,
            -0.04,
            -0.04,
            -0.06,
            -0.07,
            -0.085,
            -0.1,
            -0.09,
            -0.055,
            -0.055,
            -0.035,
            -0.035,
            -0.045,
            -0.025,
            -0.03,
            -0.035,
            -0.045,
            -0.07,
            -0.07,
            -0.06,
            -0.05,
            -0.055,
            -0.065,
            -0.075,
            -0.085,
            -0.075,
            -0.065,
            -0.075,
            -0.08,
            -0.08,
            -0.08,
            -0.065,
            -0.06,
            -0.06,
            -0.065,
            -0.06,
            -0.065,
            -0.06,
            -0.05,
            -0.05,
            -0.06,
            -0.075,
            -0.07,
            -0.055,
            -0.055,
            -0.085,
            -0.105,
            -0.11,
            -0.075,
            -0.035,
            0.01,
            0.095,
            0.235,
            0.43,
            0.67,
            0.91,
            1.16,
            1.365,
            1.435,
            1.38,
            1.17,
            0.82,
            0.38,
            -0.03
            , -0.345
            , -0.475
            , -0.48
            , -0.41
            , -0.3
            , -0.195
            , -0.1
            , -0.04
            , -0.02
            , -0.01
            , -0.02
            , -0.035
            , -0.04
            , -0.045
            , -0.05
            , -0.045
            , -0.04
            , -0.055
            , -0.06
            , -0.06
            , -0.075
            , -0.055
            , -0.05
            , -0.05
            , -0.06
            , -0.06
            , -0.06
            , -0.05
            , -0.045
            , -0.055
            , -0.065
            , -0.06
            , -0.055
            , -0.045
            , -0.03
            , -0.045
            , -0.04
            , -0.05
            , -0.05, -0.035, -0.025, -0.02, -0.04, -0.04, -0.04, -0.035, -0.025, -0.02,
            -0.02, -0.02, -0.02, -0.005, 0.005, -0.005, 0.00, -0.005, 0.00, 0.005, 0.035, 0.03, 0.035, 0.025,
            0.03, 0.055, 0.08, 0.06, 0.065, 0.075, 0.08, 0.11, 0.14, 0.135, 0.15, 0.155,
            0.155, 0.18, 0.2, 0.205, 0.21, 0.22, 0.225, 0.24, 0.265, 0.275,
            0.26, 0.27, 0.275, 0.28, 0.3, 0.3, 0.3, 0.3, 0.295,
            0.305, 0.32, 0.295, 0.285, 0.275, 0.26, 0.26, 0.26, 0.245,
            0.22, 0.205, 0.19, 0.17, 0.17, 0.14,
            0.115, 0.1, 0.085, 0.065, 0.07, 0.04, 0.03, 0.01, -0.01, 0.005, 0.05,
            0.05, 0.05, 0.05, 0.05,0.05,0.05,0.05,0.05,0.035,
            0.035,0.05, 0.065,0.065,
            0.04,
            0.045,
            0.035,
            0.055,
            0.07,
            0.075,
            0.065,
            0.055,
            0.07,
            0.08,
            0.09,
            0.085,
            0.09,
            0.085,
            0.085,
            0.105,
            0.12,
            0.125,
            0.12,
            0.125,
            0.135,
            0.145,
            0.16,
            0.165,
            0.175,
            0.17,
            0.18,
            0.21,
            0.23,
            0.235,
            0.24,
            0.255,
            0.255,
            0.275,
            0.3,
            0.295,
            0.315,
            0.315,
            0.335,
            0.35,
            0.35,
            0.355,
            0.355,
            0.35,
            0.355,
            0.37,
            0.39,
            0.395,
            0.385,
            0.37,
            0.365,
            0.38,
            0.385,
            0.375,
            0.37,
            0.345,
            0.345,
            0.335,
            0.33,
            0.31,
            0.28,
            0.265,
            0.25,
            0.24,
            0.225,
            0.205,
            0.18,
            0.165,
            0.145,
            0.135,
            0.13,
            0.125,
            0.105,
            0.085,
            0.075,
            0.08,
            0.08,
            0.075,
            0.06,
            0.04,
            0.04,
            0.05,
            0.065,
            0.05,
            0.04,
            0.03,
            0.03,
            0.035,
            0.045,
            0.045,
            0.035,
            0.025,
            0.015,
            0.02,
            0.035,
            0.045,
            0.045,
            0.04,
            0.025,
            0.045,
            0.06,
            0.06,
            0.045,
            0.04,
            0.05,
            0.055,
            0.07,
            0.07,
            0.07,
            0.05,
            0.055,
            0.065,
            0.065,
            0.075,
            0.065,
            0.06,
            0.065,
            0.07,
            0.09,
            0.07,
            0.06,
            0.065,
            0.055,
            0.065,
            0.065,
            0.08,
            0.075,
            0.07,
            0.06,
            0.06,
            0.065,
            0.07,
            0.05,
            0.045,
            0.045,
            0.05,
            0.06,
            0.05,
            0.04,
            0.03,
            0.02,
            0.02,
            0.04,
            0.025,
            0.025,
            0.025,
            0.01,
            0.015,
            0.03,
            0.01,
            0.00,
            -0.01,
            -0.01,
            -0.005,
            0.005,
            0.00,
            -0.005,
            -0.02,
            -0.02,
            -0.01,
            -0.01,
            -0.01,
            -0.025,
            -0.035,
            -0.03,
            -0.02,
            -0.015,
            -0.015,
            -0.03,
            -0.045,
            -0.04,
            -0.04,
            -0.02,
            -0.035,
            -0.03,
            -0.045,
            -0.04,
            -0.025,
            -0.015,
            -0.015,
            -0.025,
            -0.035,
            -0.03,
            -0.015,
            -0.005,
            -0.01,
            -0.02,
            -0.03,
            -0.025,
            -0.02,
            -0.01,
            -0.02,
            -0.015,
            -0.035,
            -0.04,
            -0.04,
            -0.04,
            -0.06,
            -0.07,
            -0.085,
            -0.1,
            -0.09,
            -0.055,
            -0.055,
            -0.035,
            -0.035,
            -0.045,
            -0.025,
            -0.03,
            -0.035,
            -0.045,
            -0.07,
            -0.07,
            -0.06,
            -0.05,
            -0.055,
            -0.065,
            -0.075,
            -0.085,
            -0.075,
            -0.065,
            -0.075,
            -0.08,
            -0.08,
            -0.08,
            -0.065,
            -0.06,
            -0.06,
            -0.065,
            -0.06,
            -0.065,
            -0.06,
            -0.05,
            -0.05,
            -0.06,
            -0.075,
            -0.07,
            -0.055,
            -0.055,
            -0.085,
            -0.105,
            -0.11,
            -0.075,
            -0.035,
            0.01,
            0.095,
            0.235,
            0.43,
            0.67,
            0.91,
            1.16,
            1.365,
            1.435,
            1.38,
            1.17,
            0.82,
            0.38,
            -0.03
            , -0.345
            , -0.475
            , -0.48
            , -0.41
            , -0.3
            , -0.195
            , -0.1
            , -0.04
            , -0.02
            , -0.01
            , -0.02
            , -0.035
            , -0.04
            , -0.045
            , -0.05
            , -0.045
            , -0.04
            , -0.055
            , -0.06
            , -0.06
            , -0.075
            , -0.055
            , -0.05
            , -0.05
            , -0.06
            , -0.06
            , -0.06
            , -0.05
            , -0.045
            , -0.055
            , -0.065
            , -0.06
            , -0.055
            , -0.045
            , -0.03
            , -0.045
            , -0.04
            , -0.05
            , -0.05, -0.035, -0.025, -0.02, -0.04, -0.04, -0.04, -0.035, -0.025, -0.02,
            -0.02, -0.02, -0.02, -0.005, 0.005, -0.005, 0.00, -0.005, 0.00, 0.005, 0.035, 0.03, 0.035, 0.025,
            0.03, 0.055, 0.08, 0.06, 0.065, 0.075, 0.08, 0.11, 0.14, 0.135, 0.15, 0.155,
            0.155, 0.18, 0.2, 0.205, 0.21, 0.22, 0.225, 0.24, 0.265, 0.275,
            0.26, 0.27, 0.275, 0.28, 0.3, 0.3, 0.3, 0.3, 0.295,
            0.305, 0.32, 0.295, 0.285, 0.275, 0.26, 0.26, 0.26, 0.245,
            0.22, 0.205, 0.19, 0.17, 0.17, 0.14,
            0.115, 0.1, 0.085, 0.065, 0.07, 0.04, 0.03, 0.01, -0.01, 0.005});
    public final static Tuple2<String, List<Double>>  data= new Tuple2<>("mgh002", ecgData.list);
}

//class TupleSerializable implements KryoSerializable {
//    Tuple2<String, List<Double>> something;
//
//    @Override
//    public void read(Kryo kryo, Input input) {
//        x = input.readInt();
//        y = input.readInt();
//        something = kryo.readClassAndObject(input);
//
//    }
//
//    @Override
//    public void write(Kryo kryo, Output output) {
//        output.writeInt(x);
//        output.writeInt(y);
//        kryo.writeClassAndObject(output, something);
//
//    }
//}



