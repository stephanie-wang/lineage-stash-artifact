/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flinkwordcount;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import java.util.Random;


/**
 * A Sink that drops all data and periodically emits latency measurements
 */
public class DummyLatencyCountingSink<T> extends StreamSink<T> {

    private final Logger logger;

    private long startTime = System.currentTimeMillis();

    private int elementsSoFar = 0;

    private final int timestampInterval;

    private final int sentenceSize;

    private final int id;

    public DummyLatencyCountingSink(Logger log, int timestampInterval, int sentenceSize) {
        super(new SinkFunction<T>() {

            @Override
            public void invoke(T value, Context ctx) throws Exception {

            }
        });
        this.logger = log;
        this.timestampInterval = timestampInterval;
        this.sentenceSize = sentenceSize;

        Random rand = new Random();
        this.id = rand.nextInt(1000);
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
      long now = System.currentTimeMillis();
      logger.warn("LATENCY {} {} {}", this.id, element.getValue(), now);

      // Log the throughput.
      elementsSoFar++;
      if (now - startTime > 500) {
          // Each timestamp represents this.timestampInterval records.
          // Each timestamp was for a sentence of sentenceSize words, so divide
          // by that amount to get the total records seen.
          long recordsSeen = this.elementsSoFar * this.timestampInterval / this.sentenceSize;
          // Throughput in records/s.
          long throughput = recordsSeen * 1000 / (now - startTime);
          logger.warn("THROUGHPUT {} {} {} {}", this.id, element.getValue(), now, throughput);
          elementsSoFar = 0;
          startTime = now;
      }
    }
}
