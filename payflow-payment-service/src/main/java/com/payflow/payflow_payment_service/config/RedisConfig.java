package com.payflow.payflow_payment_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * CONCEPT — Why StringRedisTemplate vs RedisTemplate?
 *
 * StringRedisTemplate is a pre-configured RedisTemplate<String, String>.
 * We store our values as JSON strings, so StringRedisTemplate is the
 * simplest choice — no custom serializers needed.
 *
 * RedisTemplate<K, V> with custom serializers (e.g. Jackson2JsonRedisSerializer)
 * is better when you want to store and retrieve Java objects directly.
 * Tradeoff: convenience vs type safety.
 *
 * In production you might use RedisTemplate with a typed serializer so
 * you don't have to manually serialize/deserialize with ObjectMapper.
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }
}
