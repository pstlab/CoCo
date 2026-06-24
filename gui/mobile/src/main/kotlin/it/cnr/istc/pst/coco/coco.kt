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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

interface CoCoListener {
    fun onClassCreated(cls: CoCoClass)
    fun onRuleCreated(rl: CoCoRule)
    fun onObjectCreated(obj: CoCoObject)
}

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
    private val classes = ConcurrentHashMap<String, CoCoClass>()
    private val rules = ConcurrentHashMap<String, CoCoRule>()
    private val objects = ConcurrentHashMap<String, CoCoObject>()
    private val listeners = CopyOnWriteArrayList<CoCoListener>()
    private val objectListeners =
        ConcurrentHashMap<String, CopyOnWriteArrayList<CoCoObjectListener>>()

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
                                        listeners.forEach { l -> l.onClassCreated(cls) }
                                    }

                                    is CoCoEvent.RuleCreated -> {
                                        val rl = CoCoRule(
                                            name = event.name, content = event.content
                                        )
                                        rules[event.name] = rl
                                        listeners.forEach { l -> l.onRuleCreated(rl) }
                                    }

                                    is CoCoEvent.ObjectCreated -> {
                                        val obj = CoCoObject(
                                            id = event.id,
                                            classes = event.classes,
                                            properties = event.properties,
                                            values = event.values
                                        )
                                        objects[event.id] = obj
                                        listeners.forEach { l -> l.onObjectCreated(obj) }
                                    }

                                    is CoCoEvent.ClassesUpdated -> {
                                        val obj = objects[event.objectId]
                                        if (obj != null) {
                                            val updatedObj = obj.copy(classes = event.classes)
                                            objects[event.objectId] = updatedObj
                                            objectListeners[event.objectId]?.forEach { l ->
                                                l.onClassesUpdated(event.classes)
                                            }
                                        }
                                    }

                                    is CoCoEvent.PropertiesUpdated -> {
                                        val obj = objects[event.objectId]
                                        if (obj != null) {
                                            val updatedObj = obj.copy(properties = event.properties)
                                            objects[event.objectId] = updatedObj
                                            objectListeners[event.objectId]?.forEach { l ->
                                                l.onPropertiesUpdated(event.properties)
                                            }
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
                                            objectListeners[event.objectId]?.forEach { l ->
                                                l.onValuesUpdated(event.values, event.timestamp)
                                            }
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

    fun addListener(l: CoCoListener) {
        listeners.add(l)
    }

    fun removeListener(l: CoCoListener) {
        listeners.remove(l)
    }

    fun addListener(objectId: String, listener: CoCoObjectListener) {
        objectListeners.computeIfAbsent(objectId) { CopyOnWriteArrayList() }.add(listener)
    }

    fun removeListener(objectId: String, listener: CoCoObjectListener) {
        objectListeners[objectId]?.remove(listener)
        if (objectListeners[objectId]?.isEmpty() == true) {
            objectListeners.remove(objectId)
        }
    }
}