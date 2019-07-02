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
package ch.cern.alice.o2.kafka.testproducers;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Arrays;

import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.cern.alice.o2.kafka.utils.ToolsUtils;
import ch.cern.alice.o2.kafka.utils.ThroughputThrottler;

public class ProducerMin {
	//java -cp kafka-streams-o2-0.1-jar-with-dependencies.jar ch.cern.alice.o2.kafka.producers.Producer --record-size 100 
	//--producer-props bootstrap.servers=aido2qc41:9092,aido2qc42:9092,aido2qc43:9092 ack=0 retries=0 linger.ms=1 
	// buffer.memory=67108864 batch.size=262144 --topic test3p2r --num-records 6000000 --throughput 200000
    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String topicName = res.getString("topic");
            long numRecords = res.getLong("numRecords");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");
            String producerConfig = res.getString("producerConfigFile");
            String payloadFilePath = res.getString("payloadFile");
            String transactionalId = res.getString("transactionalId");
            boolean shouldPrintMetrics = res.getBoolean("printMetrics");
            long transactionDurationMs = res.getLong("transactionDurationMs");
            boolean transactionsEnabled =  0 < transactionDurationMs;

            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");

            if (producerProps == null && producerConfig == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }

            List<byte[]> payloadByteList = new ArrayList<byte[]>();
            if (payloadFilePath != null) {
                Path path = Paths.get(payloadFilePath);
                System.out.println("Reading payloads from: " + path.toAbsolutePath());
                if (Files.notExists(path) || Files.size(path) == 0)  {
                    throw new  IllegalArgumentException("File does not exist or empty file provided.");
                }

                String[] payloadList = new String(Files.readAllBytes(path), "UTF-8").split(payloadDelimiter);

                System.out.println("Number of messages read: " + payloadList.length);

                for (String payload : payloadList) {
                    payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
                }
            }

            Properties props = new Properties();
            if (producerConfig != null) {
                props.putAll(Utils.loadProps(producerConfig));
            }
            if (producerProps != null)
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2)
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    props.put(pieces[0], pieces[1]);
                }

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            if (transactionsEnabled)
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);

            if (transactionsEnabled)
                producer.initTransactions();

            /* setup perf test */
            //byte[] payload = null;
            /*
            Random random = new Random(0);
            if (recordSize != null) {
                payload = new byte[recordSize];
                for (int i = 0; i < payload.length; ++i)
                    payload[i] = (byte) (random.nextInt(26) + 65);
            }
            */
            ProducerRecord<byte[], byte[]> record;
            Stats stats = new Stats(numRecords, 5000);
            
            long startMs = System.currentTimeMillis();
            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);
            
            /*
            byte[] b_pre_msg = new String("latency,tagkey1=tagvalue1,tagkey2=tagvalue2 doublefield=1.0,starttime=").getBytes();
            int b_pre_msg_length = b_pre_msg.length;
            int payload_lenght = b_pre_msg.length+13;
            byte[] payload = new byte[payload_lenght];
            int index;
            for (index = 0; index < b_pre_msg_length; index++)
            	payload[index] = b_pre_msg[index];
            
            for (; index < payload_lenght; index++) 
            	payload[index] = 0;
            */
            
            //int n_meas = 100;
            //int max_loop2 = 100;
            //int loop1 = max_loop1;
            //int loop2 = max_loop2;
            //int i_meas = 0;
            int currentTransactionSize = 0;
            long transactionStartTime = 0;
            for (long i = 0; i < numRecords; i++) {
                if (transactionsEnabled && currentTransactionSize == 0) {
                    producer.beginTransaction();
                    transactionStartTime = System.currentTimeMillis();
                }
                
                String msg = "meas1,hostname=host_"+i%64+ ",cardid=card_"+i%256+" ";
                if( i%2 == 0)
                	msg += "d_value=-"+i%128;
                else
                	msg += "l_value="+i%128+"i";
                
                msg += " "+Long.toString(System.currentTimeMillis())+"000000";
                byte[] payload = msg.getBytes();
                
                //String str_time = Integer.toString(Instant.now().getNano());
                //String msg = message + str_time;
                //System.out.printf(msg);
                /*
                byte [] b_time = Long.toString(System.currentTimeMillis()).getBytes();
                for (int i1 = 0; i1 < b_time.length; i1++) {
                	payload[i1+b_pre_msg_length] = b_time[i1]; 
                }
                */
                //payload[b_pre_msg_length+b_time.length] = 13;
                //payload[b_pre_msg_length+b_time.length+1] = 10;
                
                /*
                if (payloadFilePath != null) {
                    payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
                }
                */
                record = new ProducerRecord<byte[], byte[]>(topicName, payload);

                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
                producer.send(record, cb);

                currentTransactionSize++;
                if (transactionsEnabled && transactionDurationMs <= (sendStartMs - transactionStartTime)) {
                    producer.commitTransaction();
                    currentTransactionSize = 0;
                }
                
                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }

            }

            if (transactionsEnabled && currentTransactionSize != 0)
                producer.commitTransaction();

            if (!shouldPrintMetrics) {
                producer.close();

                /* print final results */
                stats.printTotal();
            } else {
                // Make sure all messages are sent before printing out the stats and the metrics
                // We need to do this in a different branch for now since tests/kafkatest/sanity_checks/test_performance_services.py
                // expects this class to work with older versions of the client jar that don't support flush().
                producer.flush();

                /* print final results */
                stats.printTotal();

                /* print out metrics */
                ToolsUtils.printMetrics(producer.metrics());
                producer.close();
            }
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        @SuppressWarnings("deprecation")
		ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --record-size or --payload-file must be specified but not both.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-records")
                .action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");
        
        payloadOptions.addArgument("--record-size")
		        .action(store())
		        .required(false)
		        .type(Integer.class)
		        .metavar("RECORD-SIZE")
		        .dest("recordSize")
		        .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");


        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
                        "Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec");

        parser.addArgument("--producer-props")
                 .nargs("+")
                 .required(false)
                 .metavar("PROP-NAME=PROP-VALUE")
                 .type(String.class)
                 .dest("producerConfig")
                 .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
                         "These configs take precedence over those passed via --producer.config.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG-FILE")
                .dest("producerConfigFile")
                .help("producer config properties file.");

        parser.addArgument("--print-metrics")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("PRINT-METRICS")
                .dest("printMetrics")
                .help("print out metrics at the end of the test.");

        parser.addArgument("--transactional-id")
               .action(store())
               .required(false)
               .type(String.class)
               .metavar("TRANSACTIONAL-ID")
               .dest("transactionalId")
               .setDefault("performance-producer-default-transactional-id")
               .help("The transactionalId to use if transaction-duration-ms is > 0. Useful when testing the performance of concurrent transactions.");

        parser.addArgument("--transaction-duration-ms")
               .action(store())
               .required(false)
               .type(Long.class)
               .metavar("TRANSACTION-DURATION")
               .dest("transactionDurationMs")
               .setDefault(0L)
               .help("The max age of each transaction. The commitTransaction will be called after this time has elapsed. Transactions are only enabled if this value is positive.");


        return parser;
    }

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }

}
