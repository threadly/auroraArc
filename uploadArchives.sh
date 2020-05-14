#!/bin/bash
cp build.upload build.gradle
./gradlew --warning-mode all clean build publish
git checkout build.gradle
