package it.cnr.istc.pst.coco

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CoCoIntegrationTest {
    private val cocoUser = System.getenv("COCO_USER") ?: "username"
    private val cocoPass = System.getenv("COCO_PASS") ?: "password"

    private suspend fun createLoggedInClient(): CoCo {
        val coco = CoCo()
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