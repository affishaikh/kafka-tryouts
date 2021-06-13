package com.example.kafkatryouts.serializer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serializer

class KafkaSerializer<T>: Serializer<T> {
    override fun serialize(topic: String?, data: T): ByteArray {
        return jacksonObjectMapper().writeValueAsBytes(data)
    }
}