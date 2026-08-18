package it.cnr.istc.pst.coco

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.ClientRequestException
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.receiveDeserialized
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.get
import io.ktor.client.request.patch
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.URLBuilder
import io.ktor.http.URLProtocol
import io.ktor.http.Url
import io.ktor.http.contentType
import io.ktor.http.encodedPath
import io.ktor.http.isSuccess
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
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.milliseconds

class CoCo(private val client: HttpClient, private val baseUrl: String) : CoroutineScope {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CoCo::class.java)
    }

    private val supervisor = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + supervisor

    private val parsedUrl = Url(baseUrl)

    @Volatile
    private var token: AuthTokens? = null
    private val authClient = client.config {
        install(Auth) {
            bearer {
                loadTokens {
                    token?.let { BearerTokens(it.accessToken, it.refreshToken) }
                }

                refreshTokens {
                    logger.trace("Token expired, attempting refresh...")
                    refreshToken()
                    token?.let { BearerTokens(it.accessToken, it.refreshToken) }
                }

                sendWithoutRequest { request ->
                    val isCorrectHost = request.url.host == Url(baseUrl).host
                    val isPublicEndpoint = request.url.encodedPath.startsWith("/login") || request.url.encodedPath.startsWith("/refresh_token")
                    isCorrectHost && !isPublicEndpoint
                }
            }
        }
    }

    @Volatile
    private var webSocketSession: DefaultClientWebSocketSession? = null
    private var webSocketJob: kotlinx.coroutines.Job? = null
    private val isRunning = AtomicBoolean(false)
    private val _events = MutableSharedFlow<CoCoEvent>(extraBufferCapacity = 64, onBufferOverflow = BufferOverflow.DROP_OLDEST)
    val events: SharedFlow<CoCoEvent> = _events.asSharedFlow()

    /**
     * Logs in to the CoCo server with the provided username and password.
     *
     * @param username The username for authentication.
     * @param password The password for authentication.
     */
    suspend fun login(username: String, password: String): AuthTokens {
        logger.trace("Logging in with username: {}", username)
        val response = authClient.post("$baseUrl/login") {
            contentType(ContentType.Application.Json)
            setBody(LoginRequest(username, password))
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Login failed with status: ${response.status}")
        }
        token = response.body()
        return token!!
    }

    /**
     * Sets the authentication token for the CoCo client.
     *
     * @param authTokens The AuthTokens object containing the access and refresh tokens.
     */
    fun setToken(authTokens: AuthTokens) {
        token = authTokens
    }

    /**
     * Refreshes the authentication token using the refresh token.
     *
     * @throws IllegalStateException if not logged in.
     */
    suspend fun refreshToken() {
        logger.trace("Refreshing token")
        token?.let { auth ->
            val response = authClient.post("$baseUrl/refresh_token") {
                contentType(ContentType.Application.Json)
                setBody(RefreshRequest(auth.refreshToken))
            }
            if (!response.status.isSuccess()) {
                throw IllegalStateException("Token refresh failed with status: ${response.status}")
            }
            token = response.body()
        } ?: throw IllegalStateException("Not logged in")
    }

    /**
     * Connects to the CoCo server via WebSocket and starts listening for events.
     *
     * @throws IllegalStateException if not logged in.
     */
    fun connect() {
        if (isRunning.getAndSet(true)) return

        webSocketJob = launch {
            while (isRunning.get()) {
                try {
                    val auth = token ?: throw IllegalStateException("Not logged in")

                    logger.info("Connecting to WebSocket at $baseUrl/ws")

                    client.webSocket(request = {
                        url {
                            protocol = if (parsedUrl.protocol.name == "https") URLProtocol.WSS else URLProtocol.WS
                            host = parsedUrl.host
                            port = parsedUrl.port
                            path("/ws")
                            parameters.append("token", auth.accessToken)
                        }
                    }) {
                        webSocketSession = this
                        logger.info("WebSocket connected!")

                        while (isActive) {
                            val event = receiveDeserialized<CoCoEvent>()
                            _events.emit(event)
                        }
                    }
                } catch (e: ClientRequestException) {
                    when (e.response.status.value) {
                        401 -> {
                            logger.warn("Unauthorized access. Attempting to refresh token...")
                            try {
                                refreshToken()
                                logger.info("Token refreshed successfully. Reconnecting...")
                            } catch (refreshException: Exception) {
                                logger.error("Token refresh failed: ${refreshException.message}. Stopping WebSocket connection.", refreshException)
                                isRunning.set(false)
                            }
                        }

                        else -> {
                            logger.error("WebSocket connection error: ${e.message}. Retrying in 5 seconds...", e)
                            kotlinx.coroutines.delay(5000.milliseconds)
                        }
                    }
                } catch (e: Exception) {
                    if (isRunning.get()) {
                        logger.error("WebSocket connection error: ${e.message}. Retrying in 5 seconds...", e)
                        kotlinx.coroutines.delay(5000.milliseconds)
                    }
                } finally {
                    webSocketSession = null
                }
            }
        }
    }

    /**
     * Fetches all classes from the CoCo server.
     *
     * @return A list of CoCoClass objects representing all classes.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getClasses(): List<CoCoClass> {
        logger.trace("Fetching all classes")
        check(token != null) { "Not logged in" }
        val response = authClient.get("$baseUrl/classes") {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch classes with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Fetches a specific class by its name from the CoCo server.
     *
     * @param className The name of the class to fetch.
     * @return A CoCoClass object representing the class.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getClass(className: String): CoCoClass {
        logger.trace("Fetching class with name: {}", className)
        check(token != null) { "Not logged in" }
        val response = authClient.get("$baseUrl/classes/$className") {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch class with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Creates a new class on the CoCo server.
     *
     * @param cls The CoCoClass object representing the class to create.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun createClass(cls: CoCoClass) {
        logger.trace("Creating class with name: {}", cls.name)
        check(token != null) { "Not logged in" }
        val response = authClient.post("$baseUrl/classes") {
            contentType(ContentType.Application.Json)
            setBody(cls)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to create class with status: ${response.status}")
        }
    }

    /**
     * Fetches all rules from the CoCo server.
     *
     * @return A list of CoCoRule objects representing all rules.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getRules(): List<CoCoRule> {
        logger.trace("Fetching all rules")
        check(token != null) { "Not logged in" }
        val response = authClient.get("$baseUrl/rules") {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch rules with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Fetches a specific rule by its name from the CoCo server.
     *
     * @param ruleName The name of the rule to fetch.
     * @return A CoCoRule object representing the rule.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getRule(ruleName: String): CoCoRule {
        logger.trace("Fetching rule with name: {}", ruleName)
        check(token != null) { "Not logged in" }
        val response = authClient.get("$baseUrl/rules/$ruleName") {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch rule with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Creates a new rule on the CoCo server.
     *
     * @param rule The CoCoRule object representing the rule to create.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun createRule(rule: CoCoRule) {
        logger.trace("Creating rule with name: {}", rule.name)
        check(token != null) { "Not logged in" }
        val response = authClient.post("$baseUrl/rules") {
            contentType(ContentType.Application.Json)
            setBody(rule)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to create rule with status: ${response.status}")
        }
    }

    /**
     * Fetches all objects from the CoCo server, optionally filtered by classes and properties.
     *
     * @param classes Optional set of class names to filter objects.
     * @param filters Optional map of property names to their values for filtering objects.
     * @return A list of CoCoObject objects representing the filtered objects.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getObjects(classes: Set<String>? = null, filters: Map<String, CoCoValue>? = null): List<CoCoObject> {
        logger.trace("Fetching all objects")
        check(token != null) { "Not logged in" }
        val urlBuilder = URLBuilder("$baseUrl/objects")
        classes?.forEach { urlBuilder.parameters.append("class", it) }
        filters?.forEach { (key, value) -> urlBuilder.parameters.append(key, value.toString()) }
        val response = authClient.get(urlBuilder.build()) {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch objects with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Fetches a specific object by its ID from the CoCo server.
     *
     * @param objectId The ID of the object to fetch.
     * @return A CoCoObject object representing the object.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getObject(objectId: String): CoCoObject {
        logger.trace("Fetching object with ID: {}", objectId)
        check(token != null) { "Not logged in" }
        val response = authClient.get("$baseUrl/objects/$objectId") {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch object with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Creates a new object on the CoCo server.
     *
     * @param obj The CoCoObject object representing the object to create.
     * @return The ID of the newly created object.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun createObject(obj: CoCoObject): String {
        logger.trace("Creating object with ID: {}", obj.id)
        check(token != null) { "Not logged in" }
        val response = authClient.post("$baseUrl/objects") {
            contentType(ContentType.Application.Json)
            setBody(obj)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to create object with status: ${response.status}")
        }
        return response.bodyAsText()
    }

    /**
     * Updates the properties of an existing object on the CoCo server.
     *
     * @param objectId The ID of the object to update.
     * @param properties A map of property names to their new values.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun setProperties(objectId: String, properties: Map<String, CoCoValue>) {
        logger.trace("Updating properties for object with ID: {}", objectId)
        check(token != null) { "Not logged in" }
        val response = authClient.patch("$baseUrl/objects/$objectId") {
            contentType(ContentType.Application.Json)
            setBody(properties)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to update properties with status: ${response.status}")
        }
    }

    /**
     * Fetches the data of a specific object from the CoCo server.
     *
     * @param objectId The ID of the object to fetch data for.
     * @param start Optional start time for filtering data.
     * @param end Optional end time for filtering data.
     * @return A map of value names to their corresponding lists of CoCoValue objects.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun getData(objectId: String, start: Instant? = null, end: Instant? = null): Map<String, List<CoCoValue>> {
        logger.trace("Fetching data for object with ID: {}", objectId)
        check(token != null) { "Not logged in" }
        val urlBuilder = URLBuilder("$baseUrl/objects/$objectId/data")
        start?.let { urlBuilder.parameters.append("start", it.toString()) }
        end?.let { urlBuilder.parameters.append("end", it.toString()) }
        val response = authClient.get(urlBuilder.build()) {
            contentType(ContentType.Application.Json)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to fetch data with status: ${response.status}")
        }
        return response.body()
    }

    /**
     * Updates the values of an existing object on the CoCo server.
     *
     * @param objectId The ID of the object to update.
     * @param values A map of value names to their new values.
     * @throws IllegalStateException if not logged in.
     */
    suspend fun addData(objectId: String, values: Map<String, CoCoValue>) {
        logger.trace("Updating values for object with ID: {}", objectId)
        check(token != null) { "Not logged in" }
        val response = authClient.post("$baseUrl/objects/$objectId/data") {
            contentType(ContentType.Application.Json)
            setBody(values)
        }
        if (!response.status.isSuccess()) {
            throw IllegalStateException("Failed to update values with status: ${response.status}")
        }
    }

    /**
     * Closes the CoCo connection, including the WebSocket session and HTTP client.
     */
    suspend fun close() {
        logger.trace("Closing CoCo connection")
        isRunning.set(false)
        webSocketSession?.close(CloseReason(CloseReason.Codes.NORMAL, "Client closing connection"))
        webSocketJob?.join()
    }
}