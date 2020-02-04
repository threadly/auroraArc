#!/bin/bash
cp build.upload build.gradle
./gradlew --warning-mode all clean build uploadArchives
git checkout build.gradle
