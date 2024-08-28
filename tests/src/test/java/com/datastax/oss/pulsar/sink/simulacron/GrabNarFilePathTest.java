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

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.junit.jupiter.api.Test;

/**
 * Test that surefire is passing correctly the path to the NAR file
 *
 * @author enrico.olivelli
 */
public class GrabNarFilePathTest {

  @Test
  public void test() {
    String narPath = System.getProperty("narFile");
    System.out.println("NAR file: " + narPath);
    assertNotNull(narPath);
    // please be aware that depending on how you launch maven
    // you could see here a file from the reactor (local 'target')
    // or from the repository (.m2/repository...)

    assertThat(
        narPath,
        anyOf(
            containsString("cassandra-enhanced"),
            containsString("cassandra-sink-pulsar-distribution")));
    assertThat(narPath, containsString(".nar"));
    assertTrue(new File(narPath).isFile());
  }
}
