package it.cnr.istc.pst.coco

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.patch
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.URLProtocol
import io.ktor.http.Url
import io.ktor.http.contentType
import io.ktor.http.path
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

class CoCo(
    private val client: HttpClient, private val baseUrl: String
) : CoroutineScope {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoCo::class.java)
    }

    private val supervisor = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + supervisor

    private val parsedUrl = Url(baseUrl)

    @Volatile
    private var accessToken: String? = null

    @Volatile
    private var webSocketSession: DefaultClientWebSocketSession? = null
    private var webSocketJob: kotlinx.coroutines.Job? = null
    private val isRunning = AtomicBoolean(false)
    private val _events = MutableSharedFlow<CoCoEvent>(
        extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST
    )
    val events: SharedFlow<CoCoEvent> = _events.asSharedFlow()

    /**
     * Logs in to the CoCo server with the provided username and password.
     *
     * @param username The username for authentication.
     * @param password The password for authentication.
     * @return True if login was successful, false otherwise.
     */
    suspend fun login(username: String, password: String): Boolean {
        logger.trace("Logging in with username: {}", username)
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

    /**
     * Connects to the CoCo server via WebSocket and starts listening for events.
     *
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun connect() {
        logger.trace("Connecting to WebSocket at: {}", baseUrl)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        isRunning.set(true)
        webSocketJob = launch {
            var disconnectException: Throwable? = null
            try {
                client.webSocket(request = {
                    url {
                        protocol =
                            if (parsedUrl.protocol.name == "https") URLProtocol.WSS else URLProtocol.WS
                        host = parsedUrl.host
                        port = parsedUrl.port
                        path("/ws")
                        parameters.append("token", accessToken ?: "")
                    }
                }) {
                    webSocketSession = this
                    while (isRunning.get()) {
                        val result = incoming.receiveCatching()
                        if (result.isClosed) {
                            val reason = closeReason.await()
                            logger.info(
                                "WebSocket closing: code={}, reason={}",
                                reason?.code,
                                reason?.message
                            )
                            result.exceptionOrNull()?.let {
                                logger.warn("WebSocket closed with exception: ${it.localizedMessage}")
                            }
                            break
                        }

                        val frame = result.getOrNull() ?: continue
                        when (frame) {
                            is Frame.Text -> {
                                val text = frame.readText()
                                val event = Json.decodeFromString<CoCoEvent>(text)
                                _events.emit(event)
                            }

                            else -> {
                            }
                        }
                    }
                    logger.info("WebSocket disconnected gracefully via protocol handshake.")
                }
            } catch (e: Exception) {
                logger.error("WebSocket disconnected: ${e.localizedMessage}")
                disconnectException = e
            } finally {
                webSocketSession = null
                isRunning.set(false)
                _events.tryEmit(CoCoEvent.Disconnected(exception = disconnectException))
            }
        }
    }

    /**
     * Fetches all classes from the CoCo server.
     *
     * @return A list of CoCoClass objects representing all classes.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun getClasses(): List<CoCoClass> {
        logger.trace("Fetching all classes")
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

    /**
     * Fetches a specific class by its name from the CoCo server.
     *
     * @param className The name of the class to fetch.
     * @return A CoCoClass object representing the class, or null if not found.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun getClass(className: String): CoCoClass? {
        logger.trace("Fetching class with name: {}", className)
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

    /**
     * Creates a new class on the CoCo server.
     *
     * @param cls The CoCoClass object representing the class to create.
     * @return True if the class was created successfully, false otherwise.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun createClass(cls: CoCoClass): Boolean {
        logger.trace("Creating class with name: {}", cls.name)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/classes") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
                setBody(cls)
            }
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    /**
     * Fetches all rules from the CoCo server.
     *
     * @return A list of CoCoRule objects representing all rules.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun getRules(): List<CoCoRule> {
        logger.trace("Fetching all rules")
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

    /**
     * Fetches a specific rule by its name from the CoCo server.
     *
     * @param ruleName The name of the rule to fetch.
     * @return A CoCoRule object representing the rule, or null if not found.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun getRule(ruleName: String): CoCoRule? {
        logger.trace("Fetching rule with name: {}", ruleName)
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

    /**
     * Creates a new rule on the CoCo server.
     *
     * @param rule The CoCoRule object representing the rule to create.
     * @return True if the rule was created successfully, false otherwise.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun createRule(rule: CoCoRule): Boolean {
        logger.trace("Creating rule with name: {}", rule.name)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/rules") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
                setBody(rule)
            }
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    /**
     * Fetches all objects from the CoCo server.
     *
     * @return A list of CoCoObject objects representing all objects.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun getObjects(): List<CoCoObject> {
        logger.trace("Fetching all objects")
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

    /**
     * Fetches a specific object by its ID from the CoCo server.
     *
     * @param objectId The ID of the object to fetch.
     * @return A CoCoObject object representing the object, or null if not found.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun getObject(objectId: String): CoCoObject? {
        logger.trace("Fetching object with ID: {}", objectId)
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

    /**
     * Creates a new object on the CoCo server.
     *
     * @param obj The CoCoObject object representing the object to create.
     * @return True if the object was created successfully, false otherwise.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun createObject(obj: CoCoObject): Boolean {
        logger.trace("Creating object with ID: {}", obj.id)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/objects") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
                setBody(obj)
            }
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    /**
     * Updates the properties of an existing object on the CoCo server.
     *
     * @param objectId The ID of the object to update.
     * @param properties A map of property names to their new values.
     * @return True if the properties were updated successfully, false otherwise.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun updateObjectProperties(
        objectId: String, properties: Map<String, CoCoValue>
    ): Boolean {
        logger.trace("Updating properties for object with ID: {}", objectId)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.patch("$baseUrl/objects/$objectId") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
                setBody(properties)
            }
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    /**
     * Updates the values of an existing object on the CoCo server.
     *
     * @param objectId The ID of the object to update.
     * @param values A map of value names to their new values.
     * @return True if the values were updated successfully, false otherwise.
     * @throws IllegalStateException if not logged in (accessToken is null).
     */
    suspend fun updateObjectValues(objectId: String, values: Map<String, CoCoValue>): Boolean {
        logger.trace("Updating values for object with ID: {}", objectId)
        if (accessToken == null) {
            throw IllegalStateException("Not logged in")
        }

        return try {
            client.post("$baseUrl/objects/$objectId/data") {
                contentType(ContentType.Application.Json)
                header("Authorization", "Bearer $accessToken")
                setBody(values)
            }
            true
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }

    /**
     * Closes the CoCo connection, including the WebSocket session and HTTP client.
     */
    suspend fun close() {
        logger.trace("Closing CoCo connection")
        isRunning.set(false)
        webSocketSession?.close(
            CloseReason(
                CloseReason.Codes.NORMAL, "Client closing connection"
            )
        )

        webSocketJob?.join()
    }
}