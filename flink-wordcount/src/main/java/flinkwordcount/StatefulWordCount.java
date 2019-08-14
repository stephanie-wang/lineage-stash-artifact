package flinkwordcount;

import flinkwordcount.DummyLatencyCountingSink;
import flinkwordcount.RateControlledSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class StatefulWordCount {

    private static final Logger logger  = LoggerFactory.getLogger(StatefulWordCount.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // env.getConfig().setLatencyTrackinginterval(1000);  //1s

        final int checkpointIntervalSeconds = params.getInt("checkpoint-interval", -1);
        if (checkpointIntervalSeconds > 0){
            System.out.println("Enabling checkpoint with interval:" + checkpointIntervalSeconds);
            env.enableCheckpointing(checkpointIntervalSeconds * 1000);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointIntervalSeconds * 1000);
        }

        System.out.println("Disabling chaining.");
        env.disableOperatorChaining();

        final int samplePeriod = params.getInt("sample-period", 1000);
        System.out.println("Sample period: " + samplePeriod);
        final int parallelism = params.getInt("parallelism", 1);

        final int sentenceSize = params.getInt("sentence-size", 100);
        // Duration to run the job in seconds.
        final int duration = params.getInt("duration", 60);
        final int sourceRate = params.getInt("source-rate", 12500);
        final int numSentences = duration * sourceRate;
        final DataStream<Tuple3<Long, String, Integer>> text = env.addSource(
                new RateControlledSourceFunction(
                        sourceRate,
                        sentenceSize,
                        numSentences,
                        samplePeriod))
                .uid("sentence-source")
                    .setParallelism(parallelism);

        // split up the lines in pairs (2-tuples) containing:
        // (word,1)
        DataStream<Tuple4<Long, String, Long, Integer>> counts = text.rebalance()
                .flatMap(new Tokenizer())
                .name("Splitter FlatMap")
                .uid("flatmap")
                    .setParallelism(parallelism)
                .keyBy(1)
                .flatMap(new CountWords())
                .name("Count")
                .uid("count")
                    .setParallelism(parallelism);

        // write to dummy sink
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        counts.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger, samplePeriod, sentenceSize))
                .setParallelism(parallelism);

        // execute program
        env.execute("Stateful WordCount");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static final class Tokenizer implements FlatMapFunction<Tuple3<Long,String,Integer>, Tuple4<Long, String, Long, Integer>> {
        private static final long serialVersionUID = 1L;
        private Long startTime = 0L;
        private int recordsSoFar = 0;
        private int counter = 0;

        @Override
        public void flatMap(Tuple3<Long,String,Integer> value, Collector<Tuple4<Long, String, Long, Integer>> out) throws Exception {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();
            }
            recordsSoFar++;
            counter++;
            // Split the line.
            String[] tokens = value.f1.split("\\W+");
            // emit the pairs
            for (int i=0; i<tokens.length; i++) {
                if (tokens[i].length() > 0) {
                    out.collect(new Tuple4<>(value.f0, tokens[i], 1L, value.f2));
                }
            }
            if (counter == 1000) {  // Print throughput and reset
                System.out.println("Flatmap throughput: " + ((recordsSoFar * 1000) / (System.currentTimeMillis() - startTime)));
                startTime = System.currentTimeMillis();
                counter = 0;
                recordsSoFar = 0;
            }
        }
    }

    public static final class CountWords extends RichFlatMapFunction<Tuple4<Long, String, Long, Integer>, Tuple4<Long, String, Long, Integer>> {

        private transient ReducingState<Long> count;
        private Long startTime = 0L;
        private int recordsSoFar = 0;
        private int counter = 0;

        @Override
        public void open(Configuration parameters) throws Exception {

            ReducingStateDescriptor<Long> descriptor =
                    new ReducingStateDescriptor<Long>(
                            "count", // the state name
                            new Count(),
                            BasicTypeInfo.LONG_TYPE_INFO);

            count = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple4<Long, String, Long, Integer> value, Collector<Tuple4<Long, String, Long, Integer>> out) throws Exception {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();
            }
            recordsSoFar++;
            counter++;
            count.add(value.f2);
            // Keep the timestamp (value.f0) of the new record
            if (value.f0 != -1){  // If there is an assigned timestamp
                out.collect(new Tuple4<>(value.f0, value.f1, count.get(), /*operator_id=*/value.f3));
            }
            if (counter == 100000) {  // Print throughput and reset
                System.out.println("Count throughput: " + ((recordsSoFar * 1000) / (System.currentTimeMillis() - startTime)));
                startTime = System.currentTimeMillis();
                counter = 0;
                recordsSoFar = 0;
            }
        }

        public static final class Count implements ReduceFunction<Long> {

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }

}

