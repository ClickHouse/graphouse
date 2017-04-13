Build and Run
=============
To build just run `./gradlew build`.

For local launch just run from console
```bash
./gradlew build installDist
cd build/install/graphouse/
bin/graphouse
```

IDE
---
To launch from IDE just run [GraphouseMain](../src/main/java/ru/yandex/market/graphouse/GraphouseMain.java) class. You can put your debug configuration to ```local-application.properties``` file in  [script dir](../src/main/script).



