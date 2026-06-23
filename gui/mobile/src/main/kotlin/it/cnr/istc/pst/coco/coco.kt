package it.cnr.istc.pst.coco

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.Url
import io.ktor.http.contentType
import io.ktor.http.path
import io.ktor.serialization.kotlinx.json.json
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

class CoCo(private val baseUrl: String) : CoroutineScope {

    private val supervisor = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + supervisor

    private val parsedUrl = Url(baseUrl)
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(WebSockets)
    }
    private var accessToken: String? = null
    private var webSocketSession: DefaultClientWebSocketSession? = null
    private var webSocketJob: kotlinx.coroutines.Job? = null
    private val isRunning = AtomicBoolean(false)

    suspend fun login(username: String, password: String): Boolean {
        return try {
            val response = client.post("$baseUrl/login") {
                contentType(ContentType.Application.Json)
                setBody(LoginRequest(username, password))
            }.body<LoginResponse>()

            accessToken = response.accessToken
            isRunning.set(true)
            webSocketJob = launch {
                try {
                    client.webSocket(request = {
                        url {
                            host = parsedUrl.host
                            path("/ws")
                            parameters.append("token", accessToken ?: "")
                        }
                    }) {
                        webSocketSession = this
                        while (isRunning.get()) {
                            val result = incoming.receiveCatching()
                            if (result.isClosed) {
                                break
                            }

                            val frame = result.getOrNull() ?: continue
                            when (frame) {
                                is Frame.Text -> {
                                    val text = frame.readText()
                                    when (val event = Json.decodeFromString<CoCoEvent>(text)) {
                                        is CoCoEvent.CoCo -> println(event)
                                    }
                                }

                                else -> {
                                }
                            }
                        }
                        println("WebSocket disconnected gracefully via protocol handshake.")
                    }
                } catch (e: Exception) {
                    System.err.println("WebSocket disconnected: ${e.localizedMessage}")
                } finally {
                    webSocketSession = null
                    println("WebSocket loop terminated.")
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
            client.get("$baseUrl/classes") {
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
            client.get("$baseUrl/classes/$className") {
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
            client.get("$baseUrl/rules") {
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
            client.get("$baseUrl/rules/$ruleName") {
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
            client.get("$baseUrl/objects") {
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
            client.get("$baseUrl/objects/$objectId") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
            }.body()
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    suspend fun close() {
        isRunning.set(false)
        webSocketSession?.close(
            CloseReason(
                CloseReason.Codes.NORMAL, "Client closing connection"
            )
        )

        webSocketJob?.join()
        client.close()
    }
}