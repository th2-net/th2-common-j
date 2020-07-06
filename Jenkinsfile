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
        GCHAT_WEB_HOOK = credentials('th2-dev-environment-web-hook')
        GCHAT_THREAD_NAME = credentials('th2-dev-environment-release-docker-images-thread')
    }
    stages {
        stage ('Artifactory configuration') {
            steps {
                rtGradleDeployer (
                    id: "GRADLE_DEPLOYER",
                    serverId: "artifatory5",
                    repo: "libs-snapshot-local",
                )

                rtGradleResolver (
                    id: "GRADLE_RESOLVER",
                    serverId: "artifatory5",
                    repo: "libs-snapshot"
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
                    writeJSON file: 'result.json', json: [text: fields.join('\n'), thread: [name: GCHAT_THREAD_NAME]]
                    try {
                        sh "curl -s -H 'Content-Type: application/json' -d @result.json '${GCHAT_WEB_HOOK}'"
                    } catch(e) {
                        println "Exception occurred: ${e}"
                    }

                    currentBuild.description = "lib-version = ${version}"
                }
            }
        }
    }
}
