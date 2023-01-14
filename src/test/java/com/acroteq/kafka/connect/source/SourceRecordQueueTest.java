/*
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * For more information, please refer to <http://unlicense.org/>
 */
package com.acroteq.kafka.connect.source;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** @author <a href="mailto:github@adamcc.ch">Adam Crowther</a> */
@ExtendWith(MockitoExtension.class)
class SourceRecordQueueTest {

    private static final String TOPIC = "topic";
    private static final int MAX_BATCH_SIZE = 5;
    private static final int MAX_BATCH_INTERVAL_MS = 200;

    @Mock private Schema schema;

    @Test
    public void testGetBatch_moreAvailableThanBatchSize() {
        // given:
        final SourceRecordQueue sourceRecordQueue = new SourceRecordQueue(MAX_BATCH_SIZE, MAX_BATCH_INTERVAL_MS);
        IntStream.range(1, MAX_BATCH_SIZE * 2 + 2)
                 .mapToObj(this::createSourceRecord)
                 .forEach(sourceRecordQueue::add);

        assertReturnsBeforeBatchInterval(sourceRecordQueue,
                                         new String[] { "Value 1", "Value 2", "Value 3", "Value 4", "Value 5" });
        assertReturnsBeforeBatchInterval(sourceRecordQueue,
                                         new String[] { "Value 6", "Value 7", "Value 8", "Value 9", "Value 10" });
        assertReturnsAfterBatchInterval(sourceRecordQueue, new String[] { "Value 11" });
        assertReturnsAfterBatchInterval(sourceRecordQueue, new String[] {});
    }

    @Test
    public void testGetBatch_noneAvailable() {
        // given:
        final SourceRecordQueue sourceRecordQueue = new SourceRecordQueue(MAX_BATCH_SIZE, MAX_BATCH_INTERVAL_MS);

        assertReturnsAfterBatchInterval(sourceRecordQueue, new String[] {});
    }

    @NotNull
    private static List<String> getBatchValues(final List<SourceRecord> batch) {
        return batch.stream()
                    .map(SourceRecord::value)
                    .map(String.class::cast)
                    .collect(toList());
    }

    private SourceRecord createSourceRecord(final int i) {
        return new SourceRecord(emptyMap(), emptyMap(), TOPIC, schema, "Value " + i);
    }

    private void assertReturnsBeforeBatchInterval(final SourceRecordQueue sourceRecordQueue,
                                                  final String[] expectedValues) {
        await().atMost(100, MILLISECONDS)
               .pollInterval(5, MILLISECONDS)
               .until(() -> getBatchReturnedExpectedValues(sourceRecordQueue, expectedValues));
    }

    private void assertReturnsAfterBatchInterval(final SourceRecordQueue sourceRecordQueue,
                                                 final String[] expectedValues) {
        await().atLeast(100, MILLISECONDS)
               .pollInterval(5, MILLISECONDS)
               .until(() -> getBatchReturnedExpectedValues(sourceRecordQueue, expectedValues));
    }

    private static boolean getBatchReturnedExpectedValues(final SourceRecordQueue sourceRecordQueue,
                                                          final String[] expectedValues) {
        // when:
        final List<SourceRecord> batch = sourceRecordQueue.getBatch();
        // then:
        if (expectedValues.length > 0) {
            assertThat(getBatchValues(batch), contains(expectedValues));
        } else {
            assertThat(batch, is(empty()));
        }

        return true;
    }
}