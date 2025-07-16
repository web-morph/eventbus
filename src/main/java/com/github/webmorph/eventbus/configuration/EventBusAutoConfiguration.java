package com.github.webmorph.eventbus.configuration;

import com.github.webmorph.eventbus.EventBus;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@AutoConfiguration
@ComponentScan("com.github.webmorph.eventbus")
public class EventBusAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public EventBus eventBus() {
        return EventBus.GLOBAL;
    }
}
