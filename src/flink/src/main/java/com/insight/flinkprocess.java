/*
 * Flink job for detecting arryhthmia from raw ECG signals from MLII
 *
 * @author Bhautik Gandhi
 */

package com.insight;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.*;

public class FlinkProcess {
    /**
     * The process takes raw input from Kafka topic and does the following
     * 1. Extracts and assigns timestamps and watermarks
     * 2. A flatmap function that creates a tuple3 for the signal name, timestamp, and raw ecg signal value.
     * 3. Keys by the signal name (groups)
     * 4. Creates a tumbling event window for each key
     * 5. Aggregates the raw ecg signal for the duration of the window
     * 6. A flatmap function that detects anomalies

     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
     *                   to fail and may trigger recovery.
     */
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // call HostURLs class to get URLs for all services
        HostURLs urls = new HostURLs();
        // create new property of Flink
        // set Kafka url
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", urls.KAFKA_URL);
        //acknowledge all messages so as to not drop any signals.
        //properties.setProperty("acks", "all");
        properties.setProperty("group.id", "flink_consumer");
        //Create the kafka consumer
        FlinkKafkaConsumer010<Tuple3<String,String, Double>> kafkaConsumer = new FlinkKafkaConsumer010<>("ecg-topic2",
                new EcgRecSchema(), properties);
        //Read from begining
        kafkaConsumer.setStartFromEarliest();
        //read from the topic, extract timestamps and assign watermark to each message. Keeps streams in order
        DataStream<Tuple3<String, String, Double>> rawInputStream = env.addSource(kafkaConsumer)
                                                .rebalance()
                                                .assignTimestampsAndWatermarks(new PunctuatedAssigner());

        // key by user_id/file name
        // Window of 1800 elements (5 seconds of data) with a slide of 4 seconds. In order to minimize overlap and improve performance
        // aggregate function takes input, aggregates ECG values and time stamps for each key for each window
        DataStream<Tuple3<String, String[], double[]>> output = rawInputStream
                .keyBy(t->t.f0)
                .countWindow(576, 288)
                .aggregate(new SignalAggregate());
        DataStream<String[]> finalOutput = output.flatMap(new DetectAnomaly()).name("detect anomaly");
        finalOutput.addSink(new TimeScaleDBSink()).name("TimeScaleDBSink");
        env.execute("Flink job");
    } //MAIN Function
    static class DetectAnomaly implements FlatMapFunction<Tuple3<String, String[], double[]>, String[]> {
        /**
         * Detects anomalies and returns a String array. The sink function can't serialize a tuple for some reason.
         *
         * @param value The input value tuple3(string, string[], double[]).
         * @param out   The collector for returning result values String[].
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */

        @Override
        public void flatMap(Tuple3<String, String[], double[]> value, Collector<String[]> out) throws Exception {

            double[] ecgData = value.f2;
            String[] times = value.f1;
            String sigName= value.f0;
            String[] result = new String[4];
            //create a date for the current date need this for the database timestamp
            String locDate = LocalDate.now().toString();
            //Returns a linked list of indices of R peaks
            LinkedList<Integer> peaks = Peak.findPeaks(ecgData, 50, 0.75);

            // Need at least 2 R peaks to detect anomaly
            if(peaks.size()<2){
                return;
            }

            //Creates arrays for the values of Average Teager Energy Function
            double[] AvgTeagE = new double[peaks.size()-1];
            //Creates array for R-R interval values
            double[] interval = new double[AvgTeagE.length];
            //Loop between each R-R Peak
            for(int i=0;i<peaks.size()-1 ;i++) {
                int start = peaks.get(i);
                int end = peaks.get(i + 1);
                //get the raw ecg signals between the R-R Peak
                double[] ecgSlice = Arrays.copyOfRange(ecgData, start, end + 1);
                //Initialize the Teager Energy value array for each signal
                double[] teagerNrg = new double[ecgSlice.length];
                //For each ecg raw value, calculate the Teager Energy
                for (int j = 0; j < ecgSlice.length - 2; j++) {
                    teagerNrg[j] = ecgSlice[j + 1] * ecgSlice[j + 1] - ecgSlice[j] * ecgSlice[j + 2];
                }
                // for this R-R Peak, get the average Teager energy value
                AvgTeagE[i] = Arrays.stream(teagerNrg).sum() / teagerNrg.length;
                Double AvgTeagVal= AvgTeagE[i];
                //Calculate the R-R interval in seconds, fs = 360 Hz
                interval[i] = teagerNrg.length / 360.0;
                Double rrInterval = interval[i];
                //Create a time stamp for this R-R Peak
                String time = times[i];
                String timestamp = locDate + " "+ time;

                // If ane < 0.02, abnormality detected
                if (AvgTeagVal < 0.018) {
                    String ecgSliceStr =Arrays.toString(ecgSlice);
                    result[0] = sigName ;
                    result[1] = timestamp ;
                    result[2] = ecgSliceStr ;
                    result[3] = "TRUE";
                    out.collect(result);
                }
                // If rrInterval < 0.4, abnormality detected
                else if (rrInterval < 0.4) {
                    String ecgSliceStr =Arrays.toString(ecgSlice);
                    result[0] = sigName ;
                    result[1] = timestamp ;
                    result[2] = ecgSliceStr ;
                    result[3] = "TRUE";
                    out.collect(result);
                }
                //Else its normal
                else {
                }
            }// For Loop
//            endTime = System.nanoTime() - startTime;
//            System.out.println("find Anomaly time = " + endTime/1000000.0 + " ms");
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
    public static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Tuple3<String, String, Double>> {

        static final String FORMAT = "HH:mm:ss.SSS";

        @Override
        public long extractTimestamp(Tuple3<String, String, Double> element, long previousElementTimestamp) {
            SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);
            Date date = null;
            try {
                date = formatter.parse(element.f1);

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
        public Watermark checkAndGetNextWatermark(Tuple3<String, String, Double> lastElement, long extractedTimestamp) {
            SimpleDateFormat formatter = new SimpleDateFormat(FORMAT);
            Date date = null;
            try {
                date = formatter.parse(lastElement.f1);
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
    public static class EcgRecSchema implements DeserializationSchema<Tuple3<String, String, Double>> {

        /**
         * Deserializes the byte message.
         *
         * @param message The message, as a byte array.
         * @return The deserialized message as an object (null if the message cannot be deserialized).
         */
        @Override
        public Tuple3<String, String, Double> deserialize(byte[] message) throws IOException {
            String record = new String(message);
            List<String> list = new ArrayList<>(Arrays.asList(record.split(",")));
            Double signal= Double.parseDouble(list.get(2));
            Tuple3<String, String, Double> output = new Tuple3<>(list.get(0), list.get(1), signal);
            return output;
        }

        /**
         * Method to decide whether the element signals the end of the stream. If
         * true is returned the element won't be emitted.
         *
         * @param nextElement The element to test for the end-of-stream signal.
         * @return True, if the element signals end of stream, false otherwise.
         */
        @Override
        public boolean isEndOfStream(Tuple3<String, String, Double> nextElement) {
            return false;
        }

        /**
         * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
         *
         * @return The data type produced by this function or input format.
         */
        @Override
        public TypeInformation<Tuple3<String, String, Double>> getProducedType() {
            return TypeInformation
                    .of(new TypeHint<Tuple3<String, String, Double>>() { });
        }
    } //EcgRecSchema
}//FLINK PROCESS CLASS FUNCTION CLOSE
