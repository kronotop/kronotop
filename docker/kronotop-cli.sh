#!/usr/bin/env sh

exec java --enable-native-access=ALL-UNNAMED -jar ${KR_HOME}/kronotop-cli.jar "$@"
