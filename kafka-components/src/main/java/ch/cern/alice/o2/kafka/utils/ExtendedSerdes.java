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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;

public class ExtendedSerdes extends Serdes {

	static public final class AvgPairSerde extends WrapperSerde<AvgPair> {
        public AvgPairSerde() {
            super(new AvgPairSerializer(), new AvgPairDeserializer());
        }
	}
	
	static public Serde<AvgPair> AvgPair() {
        return new AvgPairSerde();
}
}