/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.apache.kafka.test.InternalProcessorContextMockBuilder;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.apache.kafka.streams.kstream.WindowedSerdes.sessionWindowedSerdeFrom;
import static org.apache.kafka.streams.processor.MockProcessorContext.CapturedForward;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class KTableSuppressProcessorTest {
    private static final long ARBITRARY_LONG = 5L;

    private static final Change<Long> ARBITRARY_CHANGE = new Change<>(7L, 14L);

    private static class Harness<K, V> {
        private final Processor<K, Change<V>> processor;
        private final InternalProcessorContext context;
        private final MockProcessorContext processorContext;


        Harness(final Suppressed<K> suppressed,
                final Serde<K> keySerde,
                final Serde<V> valueSerde) {

            final String storeName = "test-store";

            final StateStore buffer = new InMemoryTimeOrderedKeyValueBuffer.Builder<>(storeName, keySerde, valueSerde)
                    .withLoggingDisabled()
                    .build();

            final KTableImpl<K, ?, V> parent = EasyMock.mock(KTableImpl.class);
            final Processor<K, Change<V>> processor =
                    new KTableSuppressProcessorSupplier<>((SuppressedInternal<K>) suppressed, storeName, parent).get();

            processorContext = new MockProcessorContext();
            final InternalProcessorContext context = new InternalProcessorContextMockBuilder(processorContext).build();
            context.setCurrentNode(new ProcessorNode<>("testNode"));

            buffer.init(context, buffer);
            processor.init(context);

            this.processor = processor;
            this.context = context;
        }

        List<CapturedForward> forwarded() {
            return processorContext.forwarded();
        }
    }

    @Test
    public void zeroTimeLimitShouldImmediatelyEmit() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(ZERO, unbounded()), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = ARBITRARY_LONG;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final String key = "hey";
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(key, value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void windowedZeroTimeLimitShouldImmediatelyEmit() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(untilTimeLimit(ZERO, unbounded()), timeWindowedSerdeFrom(String.class, 100L), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = ARBITRARY_LONG;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(key, value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void intermediateSuppressionShouldBufferAndEmitLater() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(ofMillis(1), unbounded()), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 0L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "topic", null));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, 1L);
        harness.processor.process(key, value);
        assertThat(harness.forwarded(), hasSize(0));

        context.setRecordContext(new ProcessorRecordContext(1L, 1L, 0, "topic", null));
        harness.processor.process("tick", new Change<>(null, null));

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(finalResults(ofMillis(1L)), timeWindowedSerdeFrom(String.class, 1L), Long());
        final InternalProcessorContext context = harness.context;

        final long windowStart = 99L;
        final long recordTime = 99L;
        final long windowEnd = 100L;
        context.setRecordContext(new ProcessorRecordContext(recordTime, 0L, 0, "topic", null));
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(windowStart, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(key, value);
        assertThat(harness.forwarded(), hasSize(0));

        // although the stream time is now 100, we have to wait 1 ms after the window *end* before we
        // emit "hey", so we don't emit yet.
        final long windowStart2 = 100L;
        final long recordTime2 = 100L;
        final long windowEnd2 = 101L;
        context.setRecordContext(new ProcessorRecordContext(recordTime2, 1L, 0, "topic", null));
        harness.processor.process(new Windowed<>("dummyKey1", new TimeWindow(windowStart2, windowEnd2)), ARBITRARY_CHANGE);
        assertThat(harness.forwarded(), hasSize(0));

        // ok, now it's time to emit "hey"
        final long windowStart3 = 101L;
        final long recordTime3 = 101L;
        final long windowEnd3 = 102L;
        context.setRecordContext(new ProcessorRecordContext(recordTime3, 1L, 0, "topic", null));
        harness.processor.process(new Windowed<>("dummyKey2", new TimeWindow(windowStart3, windowEnd3)), ARBITRARY_CHANGE);

        assertExactlyOneForwarded(harness, recordTime, key, value);
    }

    /**
     * Testing a special case of final results: that even with a grace period of 0,
     * it will still buffer events and emit only after the end of the window.
     * As opposed to emitting immediately the way regular suppression would with a time limit of 0.
     */
    @Test
    public void finalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final InternalProcessorContext context = harness.context;

        // note the record is in the past, but the window end is in the future, so we still have to buffer,
        // even though the grace period is 0.
        final long timestamp = 5L;
        final long windowEnd = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(key, value);
        assertThat(harness.forwarded(), hasSize(0));

        context.setRecordContext(new ProcessorRecordContext(windowEnd, 1L, 0, "topic", null));
        harness.processor.process(new Windowed<>("dummyKey", new TimeWindow(windowEnd, windowEnd + 100L)), ARBITRARY_CHANGE);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        harness.processor.process(key, value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    /**
     * It's desirable to drop tombstones for final-results windowed streams, since (as described in the
     * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
     */
    @Test
    public void finalResultsShouldDropTombstonesForTimeWindows() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(finalResults(ofMillis(0L)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        assertThat(harness.forwarded(), hasSize(0));
    }


    /**
     * It's desirable to drop tombstones for final-results windowed streams, since (as described in the
     * {@link SuppressedInternal} javadoc), they are unnecessary to emit.
     */
    @Test
    public void finalResultsShouldDropTombstonesForSessionWindows() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(finalResults(ofMillis(0L)), sessionWindowedSerdeFrom(String.class), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        assertThat(harness.forwarded(), hasSize(0));
    }

    /**
     * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
     * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForTimeWindows() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), timeWindowedSerdeFrom(String.class, 100L), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }


    /**
     * It's NOT OK to drop tombstones for non-final-results windowed streams, since we may have emitted some results for
     * the window before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForSessionWindows() {
        final Harness<Windowed<String>, Long> harness =
                new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), sessionWindowedSerdeFrom(String.class), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }


    /**
     * It's SUPER NOT OK to drop tombstones for non-windowed streams, since we may have emitted some results for
     * the key before getting the tombstone (see the {@link SuppressedInternal} javadoc).
     */
    @Test
    public void suppressShouldNotDropTombstonesForKTable() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(ofMillis(0), maxRecords(0)), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void suppressShouldEmitWhenOverRecordCapacity() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(Duration.ofDays(100), maxRecords(1)), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        context.setRecordContext(new ProcessorRecordContext(timestamp + 1, 1L, 0, "", null));
        harness.processor.process("dummyKey", value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void suppressShouldEmitWhenOverByteCapacity() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(Duration.ofDays(100), maxBytes(60L)), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        context.setRecordContext(new ProcessorRecordContext(timestamp + 1, 1L, 0, "", null));
        harness.processor.process("dummyKey", value);

        assertExactlyOneForwarded(harness, timestamp, key, value);
    }

    @Test
    public void suppressShouldShutDownWhenOverRecordCapacity() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(Duration.ofDays(100), maxRecords(1).shutDownWhenFull()), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        context.setCurrentNode(new ProcessorNode<>("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        context.setRecordContext(new ProcessorRecordContext(timestamp, 1L, 0, "", null));

        final StreamsException e = assertThrows(StreamsException.class, () -> harness.processor.process("dummyKey", value));
        assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
    }

    @Test
    public void suppressShouldShutDownWhenOverByteCapacity() {
        final Harness<String, Long> harness =
                new Harness<>(untilTimeLimit(Duration.ofDays(100), maxBytes(60L).shutDownWhenFull()), String(), Long());
        final InternalProcessorContext context = harness.context;

        final long timestamp = 100L;
        context.setRecordContext(new ProcessorRecordContext(timestamp, 0L, 0, "", null));
        context.setCurrentNode(new ProcessorNode<>("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        harness.processor.process(key, value);

        context.setRecordContext(new ProcessorRecordContext(timestamp, 1L, 0, "", null));

        final StreamsException e = assertThrows(StreamsException.class, () -> harness.processor.process("dummyKey", value));
        assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
    }

    private static <K, V> void assertExactlyOneForwarded(final Harness<K, V> harness, final long timestamp, final K key, final Change<V> value) {
        assertThat(harness.forwarded(), hasSize(1));
        final CapturedForward capturedForward = harness.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <K extends Windowed> SuppressedInternal<K> finalResults(final Duration grace) {
        return ((FinalResultsSuppressionBuilder) untilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
    }

    private static <E> Matcher<Collection<E>> hasSize(final int i) {
        return new BaseMatcher<Collection<E>>() {
            @Override
            public void describeTo(final Description description) {
                description.appendText("a collection of size " + i);
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean matches(final Object item) {
                if (item == null) {
                    return false;
                } else {
                    return ((Collection<E>) item).size() == i;
                }
            }

        };
    }

    private static <K> Serde<Windowed<K>> timeWindowedSerdeFrom(final Class<K> rawType, final long windowSize) {
        final Serde<K> kSerde = Serdes.serdeFrom(rawType);
        return new Serdes.WrapperSerde<>(
                new TimeWindowedSerializer<>(kSerde.serializer()),
                new TimeWindowedDeserializer<>(kSerde.deserializer(), windowSize)
        );
    }
}