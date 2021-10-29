package tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MultiProducerTest {
    private static int numRecords;
    private static Stats stats;
    static AtomicLong index;
    //阻塞队列实现生产者实例池,获取连接作出队操作，归还连接作入队操作
    public static BlockingQueue<KafkaProducer<byte[], byte[]>> queue;

    //负责发送消息的线程池
    private static ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static CountDownLatch countDownLatch;


    public static void main(String[] args) {
        index = new AtomicLong();
        ArgumentParser parser = argParser();
        try {
            Namespace res = parser.parseArgs(args);
            String topic = res.getString("topic");
            numRecords = res.getInt("numRecords");
            String producerConfig = res.getString("producerConfigFile");
            Properties properties = new Properties();
            properties.putAll(Utils.loadProps(producerConfig));
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

            int numProducers = res.getInt("numProducers");
            queue = new LinkedBlockingQueue<>(numProducers);

            for (int i = 0; i < numProducers; ++i) {
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);
                queue.add(producer);
            }
            int concurrency = res.getInt("Concurrency");
            executorService = Executors.newFixedThreadPool(concurrency);
            countDownLatch = new CountDownLatch(numRecords);

            stats = new Stats(numRecords, 5000);
            for (int i = 0; i < numRecords; i++) {
                executorService.submit(new ProducerWorker(topic));
            }
            countDownLatch.await();
        } catch (InterruptedException | ArgumentParserException | IOException e) {
            e.printStackTrace();
        } finally {
            if (queue != null) {
                while (!queue.isEmpty()) {
                    queue.poll().close();
                }
            }
            if (stats != null) {
                stats.printTotal();
            }
            if (executorService != null) {
                executorService.shutdown();
            }
        }
    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("producer-for-connect").defaultHelp(true).description("This tool is used to verify the producer for kafka connect.");
        parser.addArgument(new String[]{"--topic"}).action(Arguments.store()).required(true).type(String.class).metavar(new String[]{"TOPIC"}).help("produce messages to this topic");
        parser.addArgument(new String[]{"--num-records"}).action(Arguments.store()).required(true).type(Integer.class).metavar(new String[]{"NUM-RECORDS"}).dest("numRecords").help("number of messages to produce");
        parser.addArgument(new String[]{"--payload-file"}).action(Arguments.store()).required(false).type(String.class).metavar(new String[]{"PAYLOAD-FILE"}).dest("payloadFile").help("file to read the message payloads from. This works only for UTF-8 encoded text files. Payloads will be read from this file and a payload will be randomly selected when sending messages. Note that you must provide exactly one of --record-size or --payload-file.");
        parser.addArgument(new String[]{"--producer.config"}).action(Arguments.store()).required(false).type(String.class).metavar(new String[]{"CONFIG-FILE"}).dest("producerConfigFile").help("producer config properties file.");
        parser.addArgument(new String[]{"--concurrency-produce"}).setDefault(Runtime.getRuntime().availableProcessors()).action(Arguments.store()).required(false).type(Integer.class).metavar(new String[]{"CONCURRENCY-PRODUCE"}).dest("Concurrency").help("Maximum concurrency of production threads");
        parser.addArgument(new String[]{"--num-producer"}).setDefault(3).action(Arguments.store()).required(false).type(Integer.class).metavar(new String[]{"NUM-PRODUCERS"}).dest("numProducers").help("number of producer");

        return parser;
    }

    private static class ProducerWorker implements Runnable {
        private ProducerRecord<byte[], byte[]> record;
        public ProducerWorker(String topic) {
            this.record = RecordReader.getRecord(topic,index.getAndIncrement());
        }

        public ProducerWorker(ProducerRecord<byte[], byte[]> record) {
            this.record = record;
        }

        @Override
        public void run() {
            try {
                KafkaProducer<byte[], byte[]> producer = queue.take();//从实例池获取连接,没有空闲连接则阻塞等待
                long sendStartMs = System.currentTimeMillis();
                synchronized (stats) {
                    Callback cb = stats.nextCompletion(sendStartMs, record.key().length + record.value().length, stats);
                    producer.send(record, cb);
                }
                queue.put(producer);//归还kafka连接到连接池队列
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final long iteration;
        private final long bytes;
        private final Stats stats;

        public PerfCallback(long iter, long start, long bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - this.start);
            this.stats.record(this.iteration, latency, this.bytes, now);
            if (exception != null) {
                exception.printStackTrace();
            }

        }
    }

    private static class Stats {
        private long start;
        private AtomicLong count;
        private AtomicLong bytes;

        private Long windowStart;
        private AtomicLong windowCount;
        private AtomicLong windowBytes;

        private long reportingInterval;
        private AtomicLong iteration;

        private AtomicLong maxLatency;
        private AtomicLong totalLatency;
        private AtomicLong windowMaxLatency;
        private AtomicLong windowTotalLatency;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.count = new AtomicLong();
            this.bytes = new AtomicLong();

            this.windowStart = System.currentTimeMillis();
            this.windowCount = new AtomicLong();
            this.windowBytes = new AtomicLong();

            this.reportingInterval = reportingInterval;

            this.iteration = new AtomicLong();

            this.maxLatency = new AtomicLong();
            this.totalLatency = new AtomicLong();
            this.windowMaxLatency = new AtomicLong();
            this.windowTotalLatency = new AtomicLong();
        }

        public void record(long iter, int latency, long bytes, long time) {
            this.count.incrementAndGet();
            this.bytes.addAndGet((long) bytes);

            this.windowCount.incrementAndGet();
            this.windowBytes.addAndGet((long) bytes);

            this.totalLatency.addAndGet((long) latency);
            this.windowTotalLatency.addAndGet((long) latency);

            this.maxLatency.updateAndGet(value -> Math.max(value, (long) latency));
            this.windowMaxLatency.updateAndGet(value -> Math.max(value, (long) latency));

            synchronized (windowStart) {
                if (time - this.windowStart >= this.reportingInterval) {
                    this.printWindow();
                    this.newWindow();
                }
            }
        }

        public Callback nextCompletion(long start, long bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration.getAndIncrement(), start, bytes, stats);
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - this.windowStart;
            double recsPerSec = 1000.0D * (double) this.windowCount.get() / (double) ellapsed;
            double mbPerSec = 1000.0D * (double) this.windowBytes.get() / (double) ellapsed / 1048576.0D;
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n", this.windowCount.get(), recsPerSec, mbPerSec, (double) this.windowTotalLatency.get() / (double) this.windowCount.get(), (double) this.windowMaxLatency.get());
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount.set(0);
            this.windowMaxLatency.set(0);
            this.windowTotalLatency.set(0);
            this.windowBytes.set(0);
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - this.start;
            double recsPerSec = 1000.0D * (double) this.count.get() / (double) elapsed;
            double mbPerSec = 1000.0D * (double) this.bytes.get() / (double) elapsed / 1048576.0D;
            System.out.printf("Total:%d records sent, %f records/sec (%.2f MB/sec), " +
                    "%.2f ms avg latency, %.2f ms max latency.%n",
                    this.count.get(), recsPerSec, mbPerSec,
                    (double) this.totalLatency.get() / (double) this.count.get(),
                    (double) this.maxLatency.get());
        }
    }
}

