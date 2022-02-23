/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.RecordMapper;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TopicConfig;
import com.datastax.oss.common.sink.state.InstanceState;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CassandraSinkTaskBatchTest {

  private CassandraSinkTask sinkTask;
  private InstanceState instanceState;
  private Record<GenericRecord> record;
  private final AtomicReference<Runnable> beforeFlush = new AtomicReference<>();
  private final AtomicReference<Runnable> afterFlush = new AtomicReference<>();
  private volatile Consumer<List<AbstractSinkRecord>> processorCallback = (list) -> {};

  @BeforeEach
  void setUp() {
    sinkTask =
        new CassandraSinkTask() {
          @Override
          protected void flush() {
            try {
              final Runnable before = beforeFlush.get();
              beforeFlush.set(null);
              if (before != null) {
                before.run();
              }
              super.flush();

              final Runnable after = afterFlush.get();
              afterFlush.set(null);
              if (after != null) {
                after.run();
              }
            } catch (Throwable t) {
              t.printStackTrace();
              throw new RuntimeException(t);
            }
          }

          @Override
          protected void process(List toProcess) {
            processorCallback.accept(toProcess);
          }
        };
    instanceState = mock(InstanceState.class);
    sinkTask.getProcessor().setInstanceState(instanceState);
    record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, "test", Schema.STRING);
  }

  @Test
  void batch_records() throws Exception {

    Map<String, String> settings = new LinkedHashMap<>();
    settings.put("topic.mytopic.ks.mytable.mapping", "c1=value");
    settings.put("topic.mytopic.ks.mytable.consistencyLevel", "ONE");

    TopicConfig topicConfig = new TopicConfig("mytopic", settings, false);
    when(instanceState.getTopicConfig("mytopic")).thenReturn(topicConfig);
    List<TableConfig> tableConfigs = new ArrayList<>(topicConfig.getTableConfigs());
    assertThat(tableConfigs.size()).isEqualTo(1);

    RecordMapper recordMapper1 = mock(RecordMapper.class);
    when(instanceState.getRecordMapper(tableConfigs.get(0))).thenReturn(recordMapper1);
    BoundStatement bs1 = mock(BoundStatement.class);
    when(recordMapper1.map(any(), any())).thenReturn(bs1);
    when(bs1.setConsistencyLevel(any())).thenReturn(bs1);

    // we are not calling sinkTask#open, so there is no automatic flush
    // flush happens only when batch size is reached
    {
      CompletableFuture<List<?>> flushResult = new CompletableFuture<>();
      CompletableFuture<List<?>> afterFlushResult = new CompletableFuture<>();
      beforeFlush.set(
          () -> {
            // flush always happens in a separate thread
            flushResult.complete(sinkTask.getIncomingList());
          });
      afterFlush.set(
          () -> {
            afterFlushResult.complete(sinkTask.getIncomingList());
          });
      sinkTask.setBatchSize(1);
      sinkTask.write(record);
      assertEquals(1, flushResult.get(10, TimeUnit.SECONDS).size());
      assertTrue(afterFlushResult.get(10, TimeUnit.SECONDS).isEmpty());
    }

    sinkTask.setBatchSize(10);
    sinkTask.write(record);
    sinkTask.write(record);
    sinkTask.write(record);
    sinkTask.write(record);
    sinkTask.write(record);
    assertEquals(5, sinkTask.getIncomingList().size());

    {
      CompletableFuture<List<?>> flushResult = new CompletableFuture<>();
      CompletableFuture<List<?>> afterFlushResult = new CompletableFuture<>();
      beforeFlush.set(
          () -> {
            flushResult.complete(sinkTask.getIncomingList());
          });
      afterFlush.set(
          () -> {
            afterFlushResult.complete(sinkTask.getIncomingList());
          });
      sinkTask.write(record);
      sinkTask.write(record);
      sinkTask.write(record);
      sinkTask.write(record);
      sinkTask.write(record);
      assertEquals(10, flushResult.get(10, TimeUnit.SECONDS).size());
      assertTrue(afterFlushResult.get(10, TimeUnit.SECONDS).isEmpty());
    }
  }
}
