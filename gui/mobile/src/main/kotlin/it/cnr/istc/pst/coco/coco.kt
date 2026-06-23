package it.cnr.istc.pst.coco

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.Url
import io.ktor.http.contentType
import io.ktor.http.path
import io.ktor.serialization.kotlinx.json.json
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.serialization.json.Json

class CoCo(private val baseUrl: String) {

    private val parsedUrl = Url(baseUrl)
    private var accessToken: String? = null
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(WebSockets)
    }

    suspend fun login(username: String, password: String): Boolean {
        return try {
            val response = client.post("$baseUrl/login") {
                contentType(ContentType.Application.Json)
                setBody(LoginRequest(username, password))
            }.body<LoginResponse>()

            accessToken = response.accessToken
            client.webSocket(request = {
                url {
                    host = parsedUrl.host
                    path("/ws")
                    parameters.append("token", accessToken ?: "")
                }
            }) {
                for (frame in incoming) {
                    when (frame) {
                        is Frame.Text -> {
                            val text = frame.readText()
                            when (val event = Json.decodeFromString<CoCoEvent>(text)) {
                                is CoCoEvent.CoCo -> println(event)
                            }
                        }

                        is Frame.Close -> {
                        }

                        else -> {
                        }
                    }
                }
            }
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    suspend fun getClasses(): List<CoCoClass> {
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/classes") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            emptyList()
        }
    }

    suspend fun getClass(className: String): CoCoClass? {
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/classes/$className") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    suspend fun getRules(): List<CoCoRule> {
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/rules") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            emptyList()
        }
    }

    suspend fun getRule(ruleName: String): CoCoRule? {
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/rules/$ruleName") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    suspend fun getObjects(): List<CoCoObject> {
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/objects") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            emptyList()
        }
    }

    suspend fun getObject(objectId: String): CoCoObject? {
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/objects/$objectId") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    fun close() {
        client.close()
    }
}