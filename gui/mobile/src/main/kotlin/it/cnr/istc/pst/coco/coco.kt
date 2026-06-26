package it.cnr.istc.pst.coco

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.patch
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
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

class CoCo(
    private val baseUrl: String = System.getenv("COCO_URL") ?: "https://coco.pst.istc.cnr.it"
) : CoroutineScope {

    val Any.logger: Logger get() = LoggerFactory.getLogger(this.javaClass)

    private val supervisor = SupervisorJob()
    override val coroutineContext: CoroutineContext = Dispatchers.Default + supervisor

    private val parsedUrl = Url(baseUrl)
    private val client = HttpClient {
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
    private val classes = ConcurrentHashMap<String, CoCoClass>()
    private val rules = ConcurrentHashMap<String, CoCoRule>()
    private val objects = ConcurrentHashMap<String, CoCoObject>()
    private val _classEvents = MutableSharedFlow<CoCoClass>()
    val classEvents = _classEvents.asSharedFlow()
    private val _ruleEvents = MutableSharedFlow<CoCoRule>()
    val ruleEvents = _ruleEvents.asSharedFlow()
    private val _objectEvents = MutableSharedFlow<CoCoObject>()
    val objectEvents = _objectEvents.asSharedFlow()
    private val objectFlows = ConcurrentHashMap<String, MutableSharedFlow<CoCoObject>>()

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

    suspend fun connect() {
        logger.trace("Connecting to WebSocket at: {}", baseUrl)
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
                                    is CoCoEvent.CoCo -> {
                                        event.classes?.forEach { (className, cls) ->
                                            classes[className] = cls.copy(name = className)
                                        }
                                        event.rules?.forEach { (ruleName, rl) ->
                                            rules[ruleName] = rl.copy(name = ruleName)
                                        }
                                        event.objects?.forEach { (objectId, obj) ->
                                            objects[objectId] = obj.copy(id = objectId)
                                        }
                                    }

                                    is CoCoEvent.ClassCreated -> {
                                        val cls = CoCoClass(
                                            name = event.name,
                                            parents = event.parents,
                                            staticProperties = event.staticProperties,
                                            dynamicProperties = event.dynamicProperties
                                        )
                                        classes[event.name] = cls
                                        _classEvents.tryEmit(cls)
                                    }

                                    is CoCoEvent.RuleCreated -> {
                                        val rl = CoCoRule(
                                            name = event.name, content = event.content
                                        )
                                        rules[event.name] = rl
                                        _ruleEvents.tryEmit(rl)
                                    }

                                    is CoCoEvent.ObjectCreated -> {
                                        val obj = CoCoObject(
                                            id = event.id,
                                            classes = event.classes,
                                            properties = event.properties,
                                            values = event.values
                                        )
                                        objects[event.id] = obj
                                        _objectEvents.tryEmit(obj)
                                    }

                                    is CoCoEvent.ClassesUpdated -> {
                                        val obj = objects[event.objectId]
                                        if (obj != null) {
                                            val updatedObj = obj.copy(classes = event.classes)
                                            objects[event.objectId] = updatedObj
                                            _objectEvents.tryEmit(updatedObj)
                                            objectFlows[event.objectId]?.tryEmit(updatedObj)
                                        }
                                    }

                                    is CoCoEvent.PropertiesUpdated -> {
                                        val obj = objects[event.objectId]
                                        if (obj != null) {
                                            val updatedObj = obj.copy(properties = event.properties)
                                            objects[event.objectId] = updatedObj
                                            _objectEvents.tryEmit(updatedObj)
                                            objectFlows[event.objectId]?.tryEmit(updatedObj)
                                        }
                                    }

                                    is CoCoEvent.ValuesUpdated -> {
                                        val obj = objects[event.objectId]
                                        if (obj != null) {
                                            val updatedObj =
                                                obj.copy(values = event.values.mapValues { (_, v) ->
                                                    TimeValue(
                                                        v, event.timestamp
                                                    )
                                                })
                                            objects[event.objectId] = updatedObj
                                            _objectEvents.tryEmit(updatedObj)
                                            objectFlows[event.objectId]?.tryEmit(updatedObj)
                                        }
                                    }
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

    fun classes(): List<CoCoClass> = classes.values.toList()
    fun `class`(className: String): CoCoClass? = classes[className]
    fun rules(): List<CoCoRule> = rules.values.toList()
    fun rule(ruleName: String): CoCoRule? = rules[ruleName]
    fun objects(): List<CoCoObject> = objects.values.toList()
    fun `object`(objectId: String): CoCoObject? = objects[objectId]

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

    suspend fun close() {
        logger.trace("Closing CoCo connection")
        isRunning.set(false)
        webSocketSession?.close(
            CloseReason(
                CloseReason.Codes.NORMAL, "Client closing connection"
            )
        )

        webSocketJob?.join()
        client.close()
    }

    fun observeObject(objectId: String): SharedFlow<CoCoObject> {
        return objectFlows.computeIfAbsent(objectId) {
            MutableSharedFlow(replay = 1, extraBufferCapacity = 32)
        }.asSharedFlow()
    }
}

object CoCoProvider {
    val instance: CoCo by lazy { CoCo() }
}