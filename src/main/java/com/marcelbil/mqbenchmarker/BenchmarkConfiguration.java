package com.marcelbil.mqbenchmarker;

import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import jakarta.jms.ConnectionFactory;

@Configuration
public class BenchmarkConfiguration {

    // Pool instellingen
    @Value("${benchmark.pool.max-connections:1000}")
    private int maxConnections;

    @Value("${benchmark.pool.max-sessions-per-connection:500}")
    private int maxSessionsPerConnection;

    // Artemis credentials direct uitlezen
    @Value("${spring.artemis.broker-url}")
    private String brokerUrl;

    @Value("${spring.artemis.user}")
    private String user;

    @Value("${spring.artemis.password}")
    private String password;

    @Bean
    public ConnectionFactory connectionFactory() {
        // Maak de Artemis factory aan met onze eigen uitgelezen variabelen
        org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory artemisFactory =
                new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(
                        brokerUrl,
                        user,
                        password
                );

        // Stop hem in de MessagingHub Pool
        JmsPoolConnectionFactory pooledFactory = new JmsPoolConnectionFactory();
        pooledFactory.setConnectionFactory(artemisFactory);
        
        // Extreem brede pool voor 1000+ verbindingen
        pooledFactory.setMaxConnections(maxConnections);
        pooledFactory.setMaxSessionsPerConnection(maxSessionsPerConnection);
        pooledFactory.setBlockIfSessionPoolIsFull(true);
        pooledFactory.setBlockIfSessionPoolIsFullTimeout(10000);

        return pooledFactory;
    }
}