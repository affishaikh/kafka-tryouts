package com.example.kafkatryouts.domain

import java.math.BigDecimal

data class Order(
    val id: String,
    val product: Product,
    val quantity: Int
)

data class Product(
    val id: String,
    val name: String,
    val amount: BigDecimal
)