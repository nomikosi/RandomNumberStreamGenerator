package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collector;

public class Main {


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final long tumblingWindowSize = params.getLong("tumblingWindowSize", 5000L);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Add a source that generates random 6-digit numbers
        SingleOutputStreamOperator<Tuple2<String, Long>> sourceStream = env.addSource(new RandomSixDigitNumberSource())
                // Assign timestamps and watermarks
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (element, recordTimestamp) -> element.f1)
                );

        // Key by the first digit
        KeyedStream<Tuple2<String, Long>, Integer> keyedStream = sourceStream
                .keyBy(value -> Integer.parseInt(value.f0.substring(0, 1)));

        // Apply a TumblingEventTimeWindow of 5 seconds and print the result
        keyedStream
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new CollectNumbersProcessWindowFunction())
                .print();

        // Execute the Flink job
        env.execute("Random 6-Digit Number Generator with TumblingEventTimeWindow");



    }

    // ProcessWindowFunction to collect numbers in a window
    public static class CollectNumbersProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, Integer, TimeWindow> {

        @Override
        public void process(Integer integer, ProcessWindowFunction<Tuple2<String, Long>, String, Integer, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> elements, org.apache.flink.util.Collector<String> collector) throws Exception {
            List<String> numbers = new ArrayList<>();
            for (Tuple2<String, Long> element : elements) {
                numbers.add(element.f0);
            }
            collector.collect("Window for key " + integer + ": " + numbers);
        }
    }


    public static class RandomSixDigitNumberSource implements SourceFunction<Tuple2<String, Long>> {
        private volatile boolean isRunning = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            while (isRunning) {
                // Generate a random 6-digit number
                int randomNumber = 100000 + random.nextInt(900000);
                // Get the current timestamp
                long timestamp = System.currentTimeMillis();
                // Collect the generated number and timestamp
                ctx.collect(new Tuple2<>(String.valueOf(randomNumber), timestamp));
                // Sleep for 100 milliseconds
                Thread.sleep(100);
                System.out.println("generated number :" + randomNumber);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

    }
}