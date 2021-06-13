package com.example.kafkatryouts.producer

import com.example.kafkatryouts.serializer.KafkaSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.HashMap

@Component
class OrderProducer<T> {
    private val sender: KafkaSender<String, T>
    private val dateFormat: SimpleDateFormat
    private val bootstrapServers = "localhost:9092"

    init {
        val props: MutableMap<String, Any> = HashMap()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "sample-producer"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaSerializer::class.java
        val senderOptions = SenderOptions.create<String, T>(props)
        sender = KafkaSender.create(senderOptions)
        dateFormat = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")
    }

    fun sendMessages(event: T, topic: String): Flux<SenderResult<String>> {
        val senderRecord = SenderRecord.create(ProducerRecord<String, T>(topic, event), UUID.randomUUID().toString())

        return sender.send(Mono.just(senderRecord))
    }
}