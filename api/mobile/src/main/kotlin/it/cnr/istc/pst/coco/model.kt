package it.cnr.istc.pst.coco

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonClassDiscriminator
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.double
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.longOrNull

@Serializable
data class LoginRequest(
    val username: String, val password: String
)

@Serializable
data class LoginResponse(
    @SerialName("access_token") val accessToken: String,
    @SerialName("refresh_token") val refreshToken: String
)

@Serializable(with = CoCoValueSerializer::class)
sealed interface CoCoValue {
    @Serializable
    object NullValue : CoCoValue

    @Serializable
    data class BoolValue(val value: Boolean) : CoCoValue

    @Serializable
    data class IntValue(val value: Long) : CoCoValue

    @Serializable
    data class FloatValue(val value: Double) : CoCoValue

    @Serializable
    data class StringValue(val value: String) : CoCoValue

    @Serializable
    data class BoolArrayValue(val value: List<Boolean>) : CoCoValue

    @Serializable
    data class IntArrayValue(val value: List<Long>) : CoCoValue

    @Serializable
    data class FloatArrayValue(val value: List<Double>) : CoCoValue

    @Serializable
    data class StringArrayValue(val value: List<String>) : CoCoValue
}

@Serializable
data class TimeValue(
    val value: CoCoValue, val timestamp: String
)

@Serializable
data class CoCoClass(
    val name: String? = null,
    val parents: List<String>? = null,
    @SerialName("static_properties") val staticProperties: Map<String, CoCoProperty>?,
    @SerialName("dynamic_properties") val dynamicProperties: Map<String, CoCoProperty>?
)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
@JsonClassDiscriminator("type")
sealed class CoCoProperty {
    @Serializable
    @SerialName("bool")
    data class BoolProperty(
        val default: Boolean? = null, val description: String? = null
    ) : CoCoProperty()

    @Serializable
    @SerialName("int")
    data class IntProperty(
        val default: Long? = null,
        val min: Long? = null,
        val max: Long? = null,
        val description: String? = null
    ) : CoCoProperty()

    @Serializable
    @SerialName("float")
    data class FloatProperty(
        val default: Double? = null,
        val min: Double? = null,
        val max: Double? = null,
        val description: String? = null
    ) : CoCoProperty()
}

@Serializable
data class CoCoRule(
    val name: String? = null,
    val content: String? = null,
)

@Serializable
data class CoCoObject(
    val id: String? = null,
    val classes: List<String>,
    val properties: Map<String, CoCoValue>? = null,
    val values: Map<String, TimeValue>? = null
)

@OptIn(ExperimentalSerializationApi::class)
@Serializable
@JsonClassDiscriminator("msg_type")
sealed class CoCoEvent {
    @Serializable
    @SerialName("coco")
    data class CoCo(
        val classes: Map<String, CoCoClass>? = null,
        val rules: Map<String, CoCoRule>? = null,
        val objects: Map<String, CoCoObject>? = null
    ) : CoCoEvent()

    @Serializable
    @SerialName("class-created")
    data class ClassCreated(
        val name: String,
        val parents: List<String>? = null,
        @SerialName("static_properties") val staticProperties: Map<String, CoCoProperty>?,
        @SerialName("dynamic_properties") val dynamicProperties: Map<String, CoCoProperty>?
    ) : CoCoEvent()

    @Serializable
    @SerialName("rule-created")
    data class RuleCreated(
        val name: String, val content: String
    ) : CoCoEvent()

    @Serializable
    @SerialName("object-created")
    data class ObjectCreated(
        val id: String,
        val classes: List<String>,
        val properties: Map<String, CoCoValue>? = null,
        val values: Map<String, TimeValue>? = null
    ) : CoCoEvent()

    @Serializable
    @SerialName("classes-updated")
    data class ClassesUpdated(
        @SerialName("object_id") val objectId: String, val classes: List<String>
    ) : CoCoEvent()

    @Serializable
    @SerialName("properties-updated")
    data class PropertiesUpdated(
        @SerialName("object_id") val objectId: String, val properties: Map<String, CoCoValue>
    ) : CoCoEvent()

    @Serializable
    @SerialName("values-updated")
    data class ValuesUpdated(
        @SerialName("object_id") val objectId: String,
        val values: Map<String, CoCoValue>,
        val timestamp: String
    ) : CoCoEvent()
}

object CoCoValueSerializer : KSerializer<CoCoValue> {
    override val descriptor: SerialDescriptor =
        kotlinx.serialization.descriptors.buildClassSerialDescriptor("CoCoValue")

    override fun deserialize(decoder: Decoder): CoCoValue {
        val input = decoder as? JsonDecoder
            ?: throw IllegalStateException("Questo serializer supporta solo il formato JSON")

        return when (val element = input.decodeJsonElement()) {
            is JsonNull -> CoCoValue.NullValue
            is JsonPrimitive -> {
                if (element.isString) {
                    CoCoValue.StringValue(element.content)
                } else {
                    element.booleanOrNull?.let { CoCoValue.BoolValue(it) }
                        ?: element.longOrNull?.let { CoCoValue.IntValue(it) }
                        ?: element.doubleOrNull?.let { CoCoValue.FloatValue(it) }
                        ?: CoCoValue.StringValue(element.content)
                }
            }

            is JsonArray -> {
                if (element.isEmpty()) {
                    CoCoValue.StringArrayValue(emptyList())
                } else {
                    val first = element.first()
                    when {
                        first is JsonPrimitive && first.isString -> CoCoValue.StringArrayValue(
                            element.map { it.jsonPrimitive.content })

                        first is JsonPrimitive && first.booleanOrNull != null -> CoCoValue.BoolArrayValue(
                            element.map { it.jsonPrimitive.boolean })

                        first is JsonPrimitive && first.longOrNull != null -> CoCoValue.IntArrayValue(
                            element.map { it.jsonPrimitive.long })

                        else -> CoCoValue.FloatArrayValue(element.map { it.jsonPrimitive.double })
                    }
                }
            }

            is JsonObject -> throw IllegalArgumentException("CoCoValue non si aspetta un intero oggetto JSON")
        }
    }

    override fun serialize(encoder: Encoder, value: CoCoValue) {
        val output = encoder as? JsonEncoder
            ?: throw IllegalStateException("Questo serializer supporta solo il formato JSON")

        val jsonElement = when (value) {
            is CoCoValue.NullValue -> JsonNull
            is CoCoValue.BoolValue -> JsonPrimitive(value.value)
            is CoCoValue.IntValue -> JsonPrimitive(value.value)
            is CoCoValue.FloatValue -> JsonPrimitive(value.value)
            is CoCoValue.StringValue -> JsonPrimitive(value.value)
            is CoCoValue.BoolArrayValue -> JsonArray(value.value.map { JsonPrimitive(it) })
            is CoCoValue.IntArrayValue -> JsonArray(value.value.map { JsonPrimitive(it) })
            is CoCoValue.FloatArrayValue -> JsonArray(value.value.map { JsonPrimitive(it) })
            is CoCoValue.StringArrayValue -> JsonArray(value.value.map { JsonPrimitive(it) })
        }
        output.encodeJsonElement(jsonElement)
    }
}