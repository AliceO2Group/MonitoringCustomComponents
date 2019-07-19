/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.cern.alice.o2.kafka.utils;

import ch.cern.alice.o2.kafka.utils.AvgPair;
import ch.cern.alice.o2.kafka.utils.AvgPairDeserializer;
import ch.cern.alice.o2.kafka.utils.AvgPairSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AvgPairSerde implements Serde<AvgPair> {

  private final Serde<AvgPair> inner;

  public AvgPairSerde() {
    inner = Serdes.serdeFrom(new AvgPairSerializer(),
                             new AvgPairDeserializer());
  }
  
  @Override
  public Serializer<AvgPair> serializer() {
    return inner.serializer();
  }

  @Override
  public Deserializer<AvgPair> deserializer() {
    return inner.deserializer();
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    inner.serializer().configure(configs, isKey);
    inner.deserializer().configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.serializer().close();
    inner.deserializer().close();
  }

}