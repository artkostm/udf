import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.github.johnrengelman.shadow") version "4.0.3"
}

group = "by.artsiom.bigdata201"
version = "1.0"

val hiveVersion: String by project
val browscapVersion: String by project

repositories {
    mavenCentral()
    maven(url = "http://conjars.org/repo")
}

dependencies {
    compileOnly("org.apache.hive", "hive-exec", hiveVersion)

    compile("com.blueconic", "browscap-java", browscapVersion)

    testCompile("junit", "junit", "4.12")
    testCompile("org.mockito", "mockito-core", "2.23.4")
    testCompile("org.apache.hive", "hive-exec", hiveVersion)
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    "shadowJar"(ShadowJar::class) {
        baseName = project.name
        classifier = null
        version = project.version.toString()
    }
}