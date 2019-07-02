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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Map;

public class AvgPairDeserializer implements Deserializer<AvgPair> {

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // do nothing
    }

    @Override
    public AvgPair deserialize(final String s, final byte[] bytes) {
    	if (bytes == null) {
            return null;
        }
    	if (bytes.length != 12) {
            throw new SerializationException("Size of data received by LongDeserializer is not 8");
}
    	return new AvgPair().deserialize(bytes);
    }

    @Override
    public void close() {

    }
}