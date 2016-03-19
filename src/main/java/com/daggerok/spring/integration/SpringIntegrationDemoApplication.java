package com.daggerok.spring.integration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.dsl.IntegrationFlowAdapter;
import org.springframework.integration.dsl.IntegrationFlowDefinition;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.dsl.support.StringStringMapBuilder;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.TriggerContext;

import java.io.File;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

@SpringBootApplication
public class SpringIntegrationDemoApplication extends IntegrationFlowAdapter {

    @Value("classpath:application.properties")
    File file;

    AtomicBoolean atomicBoolean = new AtomicBoolean();

    public static void main(String[] args) {
        SpringApplication.run(SpringIntegrationDemoApplication.class, args);
    }

    @Override
    protected IntegrationFlowDefinition<?> buildFlow() {
        return from(() -> new GenericMessage<>(file),
                e -> e.poller(pollerFactory -> pollerFactory.trigger(this::next)))
                .split(Files.splitter())
                .transform((String source) -> {
                    String[] line = source.split("=");

                    return new StringStringMapBuilder()
                            .put("key", line[0])
                            .put("value", line[1])
                            .get();
                })
                .handle(i -> {
                    System.out.println(i.getPayload());
                });
    }

    private Date next(TriggerContext triggerContext) {
        return !atomicBoolean.getAndSet(true) ? new Date() : null;
    }
}
