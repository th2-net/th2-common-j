/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

pipeline {
    agent { label "sailfish" }
    options { timestamps () }
    tools {
        jdk 'openjdk-11.0.2'
    }
    environment {
        VERSION_MAINTENANCE = """${sh(
                            returnStdout: true,
                            script: 'git rev-list --count VERSION-1.1..HEAD'
                            ).trim()}""" //TODO: Calculate revision from a specific tag instead of a root commit
        GRADLE_SWITCHES = " -Pversion_build=${BUILD_NUMBER} -Pversion_maintenance=${VERSION_MAINTENANCE}"
    }
    stages {
        stage ('Artifactory configuration') {
            steps {
                rtGradleDeployer (
                    id: "GRADLE_DEPLOYER",
                    serverId: "artifatory5",
                    repo: "th2-schema-snapshot-local",
                )

                rtGradleResolver (
                    id: "GRADLE_RESOLVER",
                    serverId: "artifatory5",
                    repo: "th2-schema-snapshot-local"
                )
            }
        }
        stage ('Config Build Info') {
            steps {
                rtBuildInfo (
                    captureEnv: true
                )
            }
        }

        stage('Build') {
            steps {
                rtGradleRun (
                    usesPlugin: true, // Artifactory plugin already defined in build script
                    useWrapper: true,
                    rootDir: "./",
                    buildFile: 'build.gradle',
                    tasks: "clean build artifactoryPublish ${GRADLE_SWITCHES}",
                    deployerId: "GRADLE_DEPLOYER",
                    resolverId: "GRADLE_RESOLVER",
                )
            }
        }
        stage ('Publish build info') {
            steps {
                rtPublishBuildInfo (
                    serverId: "artifatory5"
                )
            }
        }
        stage('Publish report') {
            steps {
                script {
                    def properties = readProperties  file: 'gradle.properties'
                    def version = "${properties['version_major'].trim()}.${properties['version_minor'].trim()}.${VERSION_MAINTENANCE}.${BUILD_NUMBER}"

                    def changeLogs = ""
                    try {
                        def changeLogSets = currentBuild.changeSets
                        for (int changeLogIndex = 0; changeLogIndex < changeLogSets.size(); changeLogIndex++) {
                            def entries = changeLogSets[changeLogIndex].items
                            for (int itemIndex = 0; itemIndex < entries.length; itemIndex++) {
                                def entry = entries[itemIndex]
                                changeLogs += "\n${entry.msg}"
                            }
                        }
                    } catch(e) {
                        println "Exception occurred: ${e}"
                    }

                    def fields = [
                        "*Job:* <${BUILD_URL}|${JOB_NAME}>",
                        "*Lib version:* ${version}",
                        "*Changes:*${changeLogs}"
                    ]

                    currentBuild.description = "lib-version = ${version}"
                }
            }
        }
    }
}
