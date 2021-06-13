package com.example.kafkatryouts.controller

import com.example.kafkatryouts.domain.Order
import com.example.kafkatryouts.producer.OrderProducer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.kafka.sender.SenderResult

const val ORDER_TOPIC = "order-topic"

@RestController
@RequestMapping("/order")
class OrderController(@Autowired val orderProducer: OrderProducer<Order>) {

    @PostMapping
    fun placeOrder(@RequestBody order: Order): Flux<Unit> {
        return orderProducer.sendMessages(order, ORDER_TOPIC).map {}
    }
}