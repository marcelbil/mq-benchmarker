package com.marcelbil.mqbenchmarker;

import jakarta.jms.*;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class BenchmarkEngine implements MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkEngine.class);

    // NIEUW: Buffer voor de laatste 15 logregels voor de webinterface
    private final ConcurrentLinkedDeque<String> uiLogs = new ConcurrentLinkedDeque<>();
    private static final int MAX_LOG_LINES = 15;

    private JmsPoolConnectionFactory pooledConnectionFactory;
    
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private AtomicBoolean isPurging = new AtomicBoolean(false);
    private AtomicInteger producedCount = new AtomicInteger(0);
    private AtomicInteger consumedCount = new AtomicInteger(0);
    private int lastProduced = 0;
    private int lastConsumed = 0;
    private int currentProducedRate = 0;
    private int currentConsumedRate = 0;
    
    private long startTime = 0;
    private int targetDurationSec = 0;
    private int targetMessageCount = 0;
    private boolean isTimeBased = false;

    private ExecutorService producerExecutor;
    private List<DefaultMessageListenerContainer> consumerContainers = new ArrayList<>();
    private String currentRole;

    public BenchmarkEngine() {
        // Geef een eerste logregel bij het opstarten
        addUiLog("Applicatie succesvol opgestart. Klaar voor gebruik.");

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    if (isRunning.get() && !isPurging.get()) {
                        int p = producedCount.get();
                        int c = consumedCount.get();
                        currentProducedRate = p - lastProduced;
                        currentConsumedRate = c - lastConsumed;
                        lastProduced = p;
                        lastConsumed = c;

                        if (isTimeBased && targetDurationSec > 0) {
                            long elapsedSec = (System.currentTimeMillis() - startTime) / 1000;
                            if (elapsedSec >= targetDurationSec) {
                                addUiLog("Tijdslimiet van " + targetDurationSec + "s bereikt. Test wordt afgebroken.");
                                stop();
                            }
                        }
                    } else {
                        currentProducedRate = 0;
                        currentConsumedRate = 0;
                    }
                } catch (InterruptedException e) {}
            }
        }).start();
    }

    // NIEUW: Helper methode om zowel naar console als UI te loggen
    private void addUiLog(String message) {
        logger.info(message); // Nog steeds in Docker log
        String time = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        uiLogs.addLast("[" + time + "] " + message);
        if (uiLogs.size() > MAX_LOG_LINES) {
            uiLogs.removeFirst();
        }
    }

    public synchronized void start(Map<String, String> config) {
        String role = config.get("role");
        addUiLog("➡️ Start-verzoek ontvangen voor Rol: " + role);

        if (isRunning.get() || isPurging.get()) {
            addUiLog("⚠️ Verzoek genegeerd: Engine draait of purget al.");
            return;
        }

        String brokerUrl = config.getOrDefault("brokerUrl", "tcp://localhost:61616");
        String username = config.getOrDefault("username", "artemis");
        String password = config.getOrDefault("password", "artemis");
        currentRole = role;
        String protocol = config.getOrDefault("protocol", "CORE");
        int queues = Integer.parseInt(config.get("queues"));
        int maxConnections = Integer.parseInt(config.getOrDefault("maxConnections", "1000"));
        int pThreads = Integer.parseInt(config.get("producerThreads"));
        int cThreads = Integer.parseInt(config.get("consumerThreads"));
        int payloadSize = Integer.parseInt(config.get("payloadSize"));
        boolean txEnabled = Boolean.parseBoolean(config.get("txEnabled"));
        boolean persistent = Boolean.parseBoolean(config.getOrDefault("persistent", "true"));
        boolean doPurge = Boolean.parseBoolean(config.getOrDefault("purgeQueues", "true"));
        
        String stopCondition = config.getOrDefault("stopCondition", "time");
        isTimeBased = stopCondition.equals("time");
        targetMessageCount = Integer.parseInt(config.getOrDefault("totalMessages", "0"));
        targetDurationSec = Integer.parseInt(config.getOrDefault("duration", "60"));

        isPurging.set(doPurge);
        isRunning.set(true); 
        startTime = System.currentTimeMillis(); 

        new Thread(() -> {
            try {
                jakarta.jms.ConnectionFactory underlyingFactory;

                if ("AMQP".equalsIgnoreCase(protocol)) {
                    String cleanUrl = brokerUrl.split("\\?")[0];
                    // FORCEER een snelle timeout bij sluiten (5 seconden ipv de default 4 minuten)
                    String amqpUrl = cleanUrl.replace("tcp://", "amqp://") + "?jms.closeTimeout=5000";
                    underlyingFactory = new org.apache.qpid.jms.JmsConnectionFactory(username, password, amqpUrl);
                    addUiLog("🔌 Verbinden via AMQP 1.0 -> " + amqpUrl);
                } else {
                    underlyingFactory = new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(brokerUrl, username, password);
                    addUiLog("🔌 Verbinden via Artemis CORE -> " + brokerUrl);
                }

                pooledConnectionFactory = new JmsPoolConnectionFactory();
                pooledConnectionFactory.setConnectionFactory(underlyingFactory);
                pooledConnectionFactory.setMaxConnections(maxConnections); 
                pooledConnectionFactory.setMaxSessionsPerConnection(500);
                
                try (Connection testConn = pooledConnectionFactory.createConnection()) {
                    testConn.start();
                }

                if (isPurging.get()) {
                    drainQueuesParallel(queues);
                    isPurging.set(false);
                }

                producedCount.set(0);
                consumedCount.set(0);
                lastProduced = 0;
                lastConsumed = 0;
                startTime = System.currentTimeMillis(); 

                addUiLog("🚀 Start Benchmark Verkeer! Rol: " + currentRole);

                if (!currentRole.equals("SENDER")) {
                    for (int i = 1; i <= queues; i++) {
                        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
                        container.setConnectionFactory(pooledConnectionFactory);
                        String listenQueue = currentRole.equals("REQUESTER") ? "benchmark.q." + i + ".REPLY" : "benchmark.q." + i;
                        container.setDestinationName(listenQueue);
                        container.setConcurrentConsumers(cThreads);
                        container.setMessageListener(this);
                        container.setSessionTransacted(txEnabled);
                        container.initialize();
                        container.start();
                        consumerContainers.add(container);
                    }
                }

                if (currentRole.equals("SENDER") || currentRole.equals("BOTH") || currentRole.equals("REQUESTER")) {
                    int totalThreads = queues * pThreads;
                    producerExecutor = Executors.newFixedThreadPool(totalThreads);
                    int msgsPerThread = isTimeBased ? Integer.MAX_VALUE : (targetMessageCount / totalThreads);

                    for (int q = 1; q <= queues; q++) {
                        final String queueName = "benchmark.q." + q;
                        for (int t = 0; t < pThreads; t++) {
                            producerExecutor.submit(() -> produceLoop(queueName, msgsPerThread, payloadSize, txEnabled, persistent));
                        }
                    }
                }
            } catch (Exception e) {
                addUiLog("❌ Fout bij het opstarten: " + e.getMessage());
                stop();
            }
        }).start();
    }

    private void drainQueuesParallel(int queues) {
        addUiLog("🧹 Start PARALLEL purge van " + queues + " queues inclusief reply-queues...");
        ExecutorService purgeExecutor = Executors.newFixedThreadPool(queues * 2); 
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicInteger totalDrained = new AtomicInteger(0);

        for (int i = 1; i <= queues; i++) {
            final int qIndex = i;
            String[] targetQueues = {"benchmark.q." + qIndex, "benchmark.q." + qIndex + ".REPLY"};
            
            for (String qName : targetQueues) {
                futures.add(CompletableFuture.runAsync(() -> {
                    if (pooledConnectionFactory == null) return;
                    try (Connection conn = pooledConnectionFactory.createConnection();
                         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                        conn.start();
                        Queue queue = session.createQueue(qName);
                        try (MessageConsumer consumer = session.createConsumer(queue)) {
                            int drained = 0;
                            while (consumer.receive(100) != null) { drained++; }
                            if (drained > 0) {
                                totalDrained.addAndGet(drained);
                            }
                        }
                    } catch (Exception e) {}
                }, purgeExecutor));
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        purgeExecutor.shutdown();
        addUiLog("✅ Purge voltooid! Totaal " + totalDrained.get() + " oude berichten verwijderd.");
    }

    private void produceLoop(String queueName, int msgsToSend, int payloadSize, boolean txEnabled, boolean persistent) {
        JmsTemplate template = new JmsTemplate(pooledConnectionFactory);
        template.setSessionTransacted(txEnabled);
        String payload = "x".repeat(Math.max(1, payloadSize));
        int batchSize = txEnabled ? 1000 : 1;
        int sent = 0;
        int deliveryMode = persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;

        while (sent < msgsToSend && isRunning.get() && !isPurging.get()) {
            int currentBatch = Math.min(batchSize, msgsToSend - sent);
            try {
                template.execute(session -> {
                    Queue queue = session.createQueue(queueName);
                    try (MessageProducer producer = session.createProducer(queue)) {
                        producer.setDeliveryMode(deliveryMode);
                        for (int i = 0; i < currentBatch; i++) {
                            TextMessage message = session.createTextMessage(payload);
                            if (currentRole.equals("BOTH") || currentRole.equals("REQUESTER")) {
                                message.setJMSReplyTo(session.createQueue(queueName + ".REPLY"));
                                message.setJMSCorrelationID(UUID.randomUUID().toString());
                            }
                            producer.send(message);
                        }
                    }
                    if (txEnabled) session.commit();
                    return null;
                });
                sent += currentBatch;
                producedCount.addAndGet(currentBatch);
            } catch (Exception e) {
                break; 
            }
        }
        
        if (!isTimeBased && (currentRole.equals("SENDER") || currentRole.equals("REQUESTER")) && producedCount.get() >= targetMessageCount) {
            isRunning.set(false);
        }
    }

    @Override
    public void onMessage(Message message) {
        consumedCount.incrementAndGet();
        try {
            if (currentRole.equals("RESPONDER") && message.getJMSReplyTo() != null) {
                JmsTemplate replyTemplate = new JmsTemplate(pooledConnectionFactory);
                replyTemplate.send(message.getJMSReplyTo(), session -> {
                    TextMessage replyMsg = session.createTextMessage("REPLY_OK");
                    replyMsg.setJMSCorrelationID(message.getJMSCorrelationID());
                    return replyMsg;
                });
                producedCount.incrementAndGet(); 
            }
        } catch (JMSException e) {}
    }

    public synchronized void stop() {
        if (!isRunning.get() && !isPurging.get()) return;
        
        isRunning.set(false);
        isPurging.set(false);
        
        addUiLog("⏳ Graceful shutdown gestart...");

        if (producerExecutor != null) {
            producerExecutor.shutdownNow(); 
        }

        if (!consumerContainers.isEmpty()) {
            addUiLog("⏳ Consumers afsluiten...");
            for (DefaultMessageListenerContainer c : consumerContainers) {
                try {
                    c.stop(); 
                    c.shutdown(); 
                } catch (Exception e) {}
            }
            consumerContainers.clear();
            addUiLog("✅ Alle consumers zijn gestopt.");
        }
        
        if (pooledConnectionFactory != null) {
            final JmsPoolConnectionFactory factoryToClose = pooledConnectionFactory;
            pooledConnectionFactory = null;
            
            new Thread(() -> {
                try {
                    factoryToClose.stop();
                    addUiLog("✅ Connectie-pool netjes gesloten.");
                } catch (Exception e) {
                    addUiLog("⚠️ Connectie-pool sloot met een timeout (AMQP geforceerd afgesloten).");
                }
            }).start();
        }
        
        addUiLog("🏁 Benchmark volledig en schoon afgesloten.");
    }

    public Map<String, Object> getStats() {
        long remaining = 0;
        if (isRunning.get() && !isPurging.get() && isTimeBased && targetDurationSec > 0) {
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            remaining = Math.max(0, targetDurationSec - elapsed);
        }
        
        return Map.of(
            "running", isRunning.get(),
            "purging", isPurging.get(),
            "produced", producedCount.get(),
            "consumed", consumedCount.get(),
            "producedRate", currentProducedRate,
            "consumedRate", currentConsumedRate,
            "timeRemaining", remaining,
            "logs", new ArrayList<>(uiLogs) // Stuur de terminal logs mee!
        );
    }
}