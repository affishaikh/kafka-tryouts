package com.example.kafkatryouts.deserializer

import com.example.kafkatryouts.domain.Order
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer

class OrderDeserializer: Deserializer<Order> {
    override fun deserialize(topic: String, data: ByteArray): Order {
        return jacksonObjectMapper().readValue(data, Order::class.java)
    }
}