package com.example.kafkatryouts.consumer

import com.example.kafkatryouts.deserializer.OrderDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import java.text.SimpleDateFormat
import javax.annotation.PostConstruct

@Component
class OrderConsumer {
    private val receiverOptions: ReceiverOptions<String, String>
    private val dateFormat: SimpleDateFormat
    private val bootstrapServers = "localhost:9092"
    private val topic = "order-topic"

    init {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        props[ConsumerConfig.CLIENT_ID_CONFIG] = "order-consumer"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "order-consumer-group"
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = OrderDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        receiverOptions = ReceiverOptions.create(props)
        dateFormat = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")
    }

    @PostConstruct
    fun postConstruct() {
        val consumer = OrderConsumer()
        consumer.consumeMessages(topic)
    }

    private fun consumeMessages(topic: String): Disposable {
        val options = receiverOptions.subscription(setOf(topic))

        val kafkaFlux = KafkaReceiver.create(options).receive()
        return kafkaFlux.doOnError {
            println("Error occurred")
            println(it)
        }.subscribe { record: ReceiverRecord<String, String> ->
            val offset = record.receiverOffset()
            println("---------------------> Consuming <-----------------------------")
            println(record.value())
            println("---------------------> Consumed <-----------------------------")
            offset.commit()
        }
    }
}
