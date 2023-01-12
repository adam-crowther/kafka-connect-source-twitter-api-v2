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

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;

@RequiredArgsConstructor
@Slf4j
class SourceRecordQueue {

    private final ConcurrentLinkedDeque<SourceRecord> sourceRecordDeque = new ConcurrentLinkedDeque<>();
    private CountDownLatch responseLatch = new CountDownLatch(1);
    private final int maxBatchSize;
    private final int maxBatchIntervalMs;

    void add(final SourceRecord sourceRecord) {
        sourceRecordDeque.add(sourceRecord);
        if (sourceRecordDeque.size() > maxBatchSize) {
            responseLatch.countDown();
        }
    }

    List<SourceRecord> getBatch() {
        waitForBatchTimeout();
        final List<SourceRecord> sourceRecords = range(0, maxBatchSize).mapToObj(i -> sourceRecordDeque.poll())
                                                                       .filter(Objects::nonNull)
                                                                       .collect(toList());
        resetBatchTimeout();
        return sourceRecords;
    }

    private void resetBatchTimeout() {
        if (sourceRecordDeque.size() < maxBatchSize) {
            responseLatch = new CountDownLatch(1);
            delayedExecutor(maxBatchIntervalMs, MILLISECONDS).execute(responseLatch::countDown);
        }
    }

    private void waitForBatchTimeout() {
        try {
            responseLatch.await(maxBatchIntervalMs, MILLISECONDS);
        } catch (final InterruptedException e) {
            log.warn("Interrupted while waiting for response lock to be released.");
        }
    }
}
