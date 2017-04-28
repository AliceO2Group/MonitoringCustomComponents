# Flume UDP/JSON Source

## Compilation
1. Install Apache Maven - https://maven.apache.org/install.html
2. Clone repository
     ```
     git clone https://github.com/AliceO2Group/MonitroingCustomComponents.git && cd flume-udp-source
     ```
3. Compile
     ```
     mvn clean package -e
     ```

## Installation
1. Move created `.jar` file from `target/` to Flume's `lib/` directory
2. Configure UDP source (see config/o2.conf)
    ```
    a1.sources = udp
    a1.sources.udp.type = ch.cern.alice.o2.flume.UDPSource
    a1.sources.udp.port = <port>
    ...
    ```
