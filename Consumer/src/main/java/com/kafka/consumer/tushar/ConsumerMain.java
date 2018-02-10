/*
 * includes the main method that includes defining consumer group,
 * consumer instances and dispatching consumers to thread pool.
 */

package com.kafka.consumer.tushar;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;

public class ConsumerMain {

    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        String groupId = "group01";
        String host = "localhost";
        int port = 41000;
        int numConsumers = 10;
        List<String> topics = Arrays.asList(args[0].toString());
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ConsumerInstance> consumerInstanceList = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            //System.out.println("DEBUG -> Creating consumerInstance instance of ConsumerInstance");
            ConsumerInstance consumerInstance = new ConsumerInstance(
                    groupId,
                    topics,
                    host,
                    port,
                    count);

            consumerInstanceList.add(consumerInstance);
            //submit consumerInstance to thread pool.
            executor.submit(consumerInstance);
        }

        //wake up consumer from the infinite polling loop.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerInstance consumerInstance : consumerInstanceList) {
                    //shutdown consumerInstance.
                    consumerInstance.shutdown();
                }

                executor.shutdown();

                try {
                    if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                        System.out.println("Timed out waiting for threads to shutdown, hence exit");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println(" --> Closing consumer process!!");
            }
        });
    }
}
