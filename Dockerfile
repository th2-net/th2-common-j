FROM gradle:6.6-jdk11 AS build
ARG release_version
ARG artifactory_user
ARG artifactory_password
ARG artifactory_deploy_repo_key
ARG artifactory_url

COPY ./ .
RUN gradle --no-daemon clean build artifactoryPublish \
    -Prelease_version=${release_version} \
    -Partifactory_user=${artifactory_user} \
    -Partifactory_password=${artifactory_password} \
    -Partifactory_deploy_repo_key=${artifactory_deploy_repo_key} \
    -Partifactory_url=${artifactory_url}

