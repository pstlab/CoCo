package it.cnr.istc.pst.coco

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue

class CoCoIntegrationTest {

    @Test
    fun testLogin() = runTest {
        val coco = CoCo("https://coco.pst.istc.cnr.it")
        val loginSuccess = coco.login("admin", "admin")
        assertTrue(loginSuccess)
    }

    @Test
    fun testGetClasses() = runTest {
        val coco = CoCo("https://coco.pst.istc.cnr.it")
        val loginSuccess = coco.login("admin", "admin")
        assertTrue(loginSuccess)

        val classes = coco.getClasses()
    }
}