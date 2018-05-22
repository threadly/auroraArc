#!/bin/bash
cp build.upload build.gradle
./gradlew clean build uploadArchives
git checkout build.gradle
