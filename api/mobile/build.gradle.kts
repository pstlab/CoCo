plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.maven.publish.vanniktech)
    id("maven-publish")
}

repositories {
    mavenCentral()
}

dependencies {
    // Core Architecture Infrastructure
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.client.websockets)
    implementation(libs.ktor.serialization.kotlinx.json)
    implementation(libs.slf4j.api)

    // Unit & Logic Flow Verifications
    testImplementation(kotlin("test"))
    testImplementation(libs.ktor.client.cio)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
}

mavenPublishing {
    coordinates("io.github.pstlab", "coco-client", "1.0.7")

    pom {
        name.set("CoCo Kotlin Client")
        description.set("A thread-safe Kotlin/Ktor client library for the CoCo cognitive architecture.")
        url.set("https://github.com/pstlab/CoCo")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("riccardodebenedictis")
                name.set("Riccardo De Benedictis")
                email.set("riccardo.debenedictis@cnr.it")
            }
        }
        scm {
            connection.set("scm:git:ssh://github.com/pstlab/CoCo.git")
            developerConnection.set("scm:git:ssh://github.com/pstlab/CoCo.git")
            url.set("https://github.com/pstlab/CoCo")
        }
    }

    publishToMavenCentral()

    signAllPublications()
}