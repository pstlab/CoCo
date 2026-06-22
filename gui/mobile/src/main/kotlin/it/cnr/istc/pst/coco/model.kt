package it.cnr.istc.pst.coco

import kotlinx.serialization.Serializable

@Serializable
data class LoginRequest(
    val username: String, val password: String
)

@Serializable
data class LoginResponse(
    val token: String
)
