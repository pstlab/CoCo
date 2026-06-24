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
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicBoolean
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class CoCo(private val baseUrl: String) : CoroutineScope {

    val Any.logger: Logger get() = LoggerFactory.getLogger(this.javaClass)

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
        logger.debug("Logging in with username: {}", username)
        return try {
            val response = client.post("$baseUrl/login") {
                contentType(ContentType.Application.Json)
                setBody(LoginRequest(username, password))
            }.body<LoginResponse>()

            accessToken = response.accessToken
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    suspend fun connect() {
        logger.debug("Connecting to WebSocket at: {}", baseUrl)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

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
                    logger.info("WebSocket disconnected gracefully via protocol handshake.")
                }
            } catch (e: Exception) {
                logger.error("WebSocket disconnected: {}", e.localizedMessage)
            } finally {
                webSocketSession = null
            }
        }
    }

    suspend fun getClasses(): List<CoCoClass> {
        logger.debug("Fetching all classes")
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
        logger.debug("Fetching class with name: {}", className)
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
        logger.debug("Fetching all rules")
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
        logger.debug("Fetching rule with name: {}", ruleName)
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
        logger.debug("Fetching all objects")
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
        logger.debug("Fetching object with ID: {}", objectId)
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
        logger.info("Closing CoCo connection")
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