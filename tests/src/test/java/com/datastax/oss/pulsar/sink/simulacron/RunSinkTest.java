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
package com.datastax.oss.pulsar.sink.simulacron;

import java.util.HashMap;
import org.junit.jupiter.api.Test;

/** @author enrico.olivelli */
public class RunSinkTest {

  @Test
  public void test() throws Exception {
    try (PulsarSinkTester tester = new PulsarSinkTester()) {
      tester.start();
      tester.deploySink(new HashMap<>());
      // the sink will be deployed and fail to connect to Cassandra
      // we do not care about this fact here
    }
  }
}
