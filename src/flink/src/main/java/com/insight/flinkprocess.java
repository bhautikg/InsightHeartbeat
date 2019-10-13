package com.insight;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


import java.util.*;


public class flinkprocess {
    /**
     * The process takes raw input from Kafka topic and does the following
     * 1. Extracts and assigns timestamps and watermarks
     * 2. A flatmap function that creates a tuple3 for the signal name, timestamp, and raw ecg signal value.
     * 3. Keys by the signal name (groups)
     * 4. Creates a tumbling event window for each key
     * 5. Aggregates the raw ecg signal for the duration of the windo
     * 6. A flatmap function that detects anomalies

     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    public static void main(String[] args) throws Exception {
        // create execution environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //Use EVENT Time for ordering streaming data
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // call HostURLs class to get URLs for all services
        HostURLs urls = new HostURLs();
        // create new property of Flink
        // set Kafka url
        Properties properties_consumer = new Properties();
        properties_consumer.setProperty("bootstrap.servers", urls.KAFKA_URL);
        //acknowledge all messages so as to not drop any signals.
        properties_consumer.setProperty("acks", "all");
        properties_consumer.setProperty("group.id", "flink_consumer");


        //Create the kafka consumer
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("ecg-topic2",
                new SimpleStringSchema(), properties_consumer);
        //Read from begining
        kafkaConsumer.setStartFromEarliest();

        //read from the topic, extract timestamps and assign watermark to each message. Keeps streams in order
        DataStream<String> rawInputStream = env.addSource(kafkaConsumer)
                                                .assignTimestampsAndWatermarks(new PunctuatedAssigner());

        
        // Set flatmap parallelism to 1 in order to not shuffle the signals coming in, need this in order to proccess the signal
        // key by user_id/file name
        // Window of 1800 elements (5 seconds of data) with a slide of 4 seconds. In order to minimize overlap and improve performance
        // aggregate function takes input, aggregates ECG values and time stamps for each key for each window

        DataStream<Tuple3<String, String[], double[]>> output = rawInputStream
                .flatMap(new Splitter()).setParallelism(1)
                .keyBy(t->t.f0)
                .countWindow(1800, 1440)
                .aggregate(new SignalAggregate());



        DataStream<String[]> finalOutput = output.flatMap(new DetectAnomaly());
        finalOutput.addSink(new TimeScaleDBSink());


        env.execute("Flink job");
    } //MAIN Function


    static class Splitter implements FlatMapFunction<String, Tuple3<String, String, Double>> {
        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param sentence The input string "signal name, timestamp(HH:mm:ss.SSS), signal value mV".
         * @param out   The collector for returning result Tuple3<String, String, Double></String,>
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        @Override
        public void flatMap(String sentence, Collector<Tuple3<String,String, Double>> out) throws Exception {
            List<String> list = new ArrayList<>(Arrays.asList(sentence.split(",")));
            Double signal= Double.parseDouble(list.get(2));
            out.collect(new Tuple3<String, String, Double>(list.get(0),list.get(1), signal));
        }
    }//SPLITTER close
    static class DetectAnomaly implements FlatMapFunction<Tuple3<String, String[], double[]>, String[]> {
        /**
         * Detects anomalies and returns a String array. The sink function can't serialize a tuple for some reason.
         *
         * @param value The input value tuple3(string, string[], double[]).
         * @param out   The collector for returning result values String[].
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        private static final String[] EMPTY_ARRAY = new String[0];

        @Override
        public void flatMap(Tuple3<String, String[], double[]> value, Collector<String[]> out) throws Exception {

            double[] ecg_data = value.f2;
            String[] times = value.f1;
            String signame= value.f0;
            String[] result = new String[4];

            //create a date for the current date need this for the database timestamp
            String loc_date = LocalDate.now().toString();

            //Returns a linked list of indices of R peaks
            LinkedList<Integer> peaks = peak.findPeaks(ecg_data, 150, 0.75);
            // Need at least 2 R peaks to detect anomaly
            if(peaks.size()<2){
                return;
            }

            //Creates arrays for the values of Average Teager Energy Function
            double[] ANE = new double[peaks.size()-1];
            //Creates array for R-R interval values
            double[] interval = new double[ANE.length];

            //Loop between each R-R peak
            for(int i=0;i<peaks.size()-1 ;i++) {
                int start = peaks.get(i);
                int end = peaks.get(i + 1);

                //get the raw ecg signals between the R-R peak
                double[] ecg_slice = Arrays.copyOfRange(ecg_data, start, end + 1);

                //Initialize the Teager Energy value array for each signal
                double[] NE = new double[ecg_slice.length];

                //For each ecg raw value, calculate the Teager Energy
                for (int j = 0; j < ecg_slice.length - 2; j++) {
                    NE[j] = ecg_slice[j + 1] * ecg_slice[j + 1] - ecg_slice[j] * ecg_slice[j + 2];
                }
                // for this R-R peak, get the average Teager energy value
                ANE[i] = Arrays.stream(NE).sum() / NE.length;
                Double ANE_Val= ANE[i];
                //Calculate the R-R interval in seconds, fs = 360 Hz
                interval[i] = NE.length / 360.0;
                Double RR = interval[i];

                //Create a time stamp for this R-R peak
                String time = times[i];
                String timestamp = loc_date + " "+ time;

                // If ane < 0.02, abnormality detected
                if (ANE_Val < 0.02) {
                    String slice_str =Arrays.toString(ecg_slice);

                    result[0] = signame ;
                    result[1] = timestamp ;
                    result[2] = slice_str ;
                    result[3] = "TRUE";

                    out.collect(result);

                }
                // If RR < 0.4, abnormality detected
                else if (RR < 0.4) {

                    String slice_str =Arrays.toString(ecg_slice);

                    result[0] = signame ;
                    result[1] = timestamp ;
                    result[2] = slice_str ;
                    result[3] = "TRUE";
                    out.collect(result);

                }
                //Else its normal
                else {
                    result[0] = signame ;
                    result[1] = timestamp ;
                    //return a signal thats 0
                    result[2] = "[0.0, 0.0]";
                    result[3] = "FALSE";
                    out.collect(result);
                    return;
                }
            }
        }
    }//DetectAnomaly close

    static class SignalAggregate implements AggregateFunction<Tuple3<String, String, Double>
            , Tuple3<String, List<String>, List<Double>>, Tuple3<String, String[], double[]>>{
        /**
         * Aggregates the raw ecg values for given window for each key
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
        public Tuple3<String, List<String>, List<Double>> createAccumulator() {
            List<Double>  temp = new ArrayList<>();
            List<String>  temp2 = new ArrayList<>();
            Tuple3<String, List<String>, List<Double>> accumulator = new Tuple3<>("", temp2, temp);
            return accumulator;
        }

        /**
         * Adds the given input value to the given accumulator, returning the
         * new accumulator value
         * Uses List Array to aggregate values
         *
         * <p>For efficiency, the input accumulator may be modified and returned.
         *
         * @param value       The value to add
         * @param accumulator The accumulator to add the value to
         */
        @Override
        public Tuple3<String, List<String>, List<Double>> add(Tuple3<String, String, Double> value,
                                                        Tuple3<String, List<String>, List<Double>>  accumulator) {

            accumulator.f0 = value.f0;
            accumulator.f1.add(value.f1);
            accumulator.f2.add(value.f2);
            return accumulator;
        }

        /**
         * Have to convert the ListArray into a primitive double[] array for serialization to the next flatmap function
         * Gets the result of the aggregation from the accumulator.
         *
         * @param accumulator The accumulator of the aggregation
         * @return The final aggregation result.
         */
        @Override
        public Tuple3<String, String[], double[]>  getResult(Tuple3<String, List<String>, List<Double>>  accumulator) {
            double[] target = new double[accumulator.f2.size()];
            for (int i = 0; i < target.length; i++) {
                target[i] = accumulator.f2.get(i);                // java 1.5+ style (outboxing)
            }

            String[] target2 = new String[accumulator.f1.size()];
            for (int i = 0; i < target2.length; i++) {
                target2[i] = accumulator.f1.get(i);                // java 1.5+ style (outboxing)
            }

            Tuple3<String, String[], double[]> result = new Tuple3<>(accumulator.f0,target2, target);
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
        public Tuple3<String, List<String>, List<Double>>  merge(Tuple3<String, List<String>, List<Double>>  a,
                                                           Tuple3<String, List<String>, List<Double>> b) {
            a.f1.addAll(b.f1);
            a.f2.addAll(b.f2);
            return a;
        }
    }//Signal aggregate close

    public static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<String> {

        String format = "HH:mm:ss.SSS";

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            String[] s =element.split(",");

            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = null;
            try {
                date = formatter.parse(s[1]);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            long mills = date.getTime();


            return mills;
        }//Extract Timestamp close

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

            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = null;
            try {
                date = formatter.parse(s[1]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            long mills = date.getTime();
            
            //Return a watermark if the current timestamp is greater than the last element
            if(mills < extractedTimestamp){
                return new Watermark(extractedTimestamp);
            }
            else {
                return null;
            }
        }// Check and get Next watermark close
    } // Punctuated Assigner Close


}//FLINK PROCESS CLASS FUNCTION CLOSE
