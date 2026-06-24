plugins {
    kotlin("jvm") version "2.4.0"
    kotlin("plugin.serialization") version "2.4.0"
    id("maven-publish")
    id("com.vanniktech.maven.publish") version "0.37.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.ktor:ktor-client-core:3.5.0")
    implementation("io.ktor:ktor-client-cio:3.5.0")
    implementation("io.ktor:ktor-client-content-negotiation:3.5.0")
    implementation("io.ktor:ktor-client-websockets:3.5.0")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.5.0")
    implementation("org.slf4j:slf4j-api:2.0.18")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("org.slf4j:slf4j-simple:2.0.18")
}

mavenPublishing {
    coordinates("io.github.pstlab", "coco-client", "1.0.0")

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