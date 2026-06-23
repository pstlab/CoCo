package it.cnr.istc.pst.coco

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue

class CoCoIntegrationTest {
    private val cocoUrl = System.getenv("COCO_URL") ?: "https://coco.pst.istc.cnr.it"
    private val cocoUser = System.getenv("COCO_USER") ?: "username"
    private val cocoPass = System.getenv("COCO_PASS") ?: "password"

    private suspend fun createLoggedInClient(): CoCo {
        val coco = CoCo(cocoUrl)
        val loginSuccess = coco.login(cocoUser, cocoPass)
        assertTrue(loginSuccess)
        return coco
    }

    @Test
    fun testLogin() = runTest {
        createLoggedInClient()
    }

    @Test
    fun testGetClasses() = runTest {
        val coco = createLoggedInClient()
        val classes = coco.getClasses()
    }

    @Test
    fun testGetRules() = runTest {
        val coco = createLoggedInClient()
        val rules = coco.getRules()
    }

    @Test
    fun testGetObjects() = runTest {
        val coco = createLoggedInClient()
        val objects = coco.getObjects()
    }
}