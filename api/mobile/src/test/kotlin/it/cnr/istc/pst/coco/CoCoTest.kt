package it.cnr.istc.pst.coco

import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CoCoIntegrationTest {
    private val baseUrl = System.getenv("COCO_URL") ?: "url"
    private val cocoUser = System.getenv("COCO_USER") ?: "username"
    private val cocoPass = System.getenv("COCO_PASS") ?: "password"

    private suspend fun createLoggedInClient(): CoCo {
        val client = HttpClient {
            install(ContentNegotiation) {
                json(Json {
                    ignoreUnknownKeys = true
                    isLenient = true
                })
            }
            install(WebSockets)
        }
        val coco = CoCo(client, baseUrl)
        val loginSuccess = coco.login(cocoUser, cocoPass)
        assertTrue(loginSuccess)
        return coco
    }

    @Test
    fun testLogin() = runTest {
        val coco = createLoggedInClient()
        coco.close()
    }

    @Test
    fun testFetchClasses() = runTest {
        val coco = createLoggedInClient()
        val classes = coco.fetchClasses()
        assertNotNull(classes)
        coco.close()
    }

    @Test
    fun testFetchRules() = runTest {
        val coco = createLoggedInClient()
        val rules = coco.fetchRules()
        assertNotNull(rules)
        coco.close()
    }

    @Test
    fun testFetchObjects() = runTest {
        val coco = createLoggedInClient()
        val objects = coco.fetchObjects()
        assertNotNull(objects)
        coco.close()
    }
}