# emqx-plugin-kafka

This is a kafka plugin for EMQX >= 5.0.

For EMQX >= 4.3, please see branch emqx-v4

For older EMQX versions, plugin development is no longer maintained.

## Release

A EMQX plugin release is a zip package including

1. A JSON format metadata file
2. A tar file with plugin's apps packed

Execute `make rel` to have the package created like:

```
_build/default/emqx_plugrel/emqx_plugin_kafka-<vsn>.tar.gz
```
## Config

Config file `/etc/emqx/emqx_plugin_kafka.conf` or `/opt/emqx/etc/emqx_plugin_kafka.conf` when run in container.

Support hook:

* client.connected
* client.disconnected
* message.publish



Example config:

```hocon
## This is a demo config in HOCON format
## The same format used by EMQX since 5.0

kafka_servers = [
  {
    host = server1
    port = 9092
  },
  {
    host = server2
    port = 9092
  },
  {
    host = server3
    port = 9092
  }
]

topic_mapping = [
  {
    mqtt_topic = "$thing/req/property/#"
    kafka_topic = mqttThingProperty
  },
  {
    mqtt_topic = "$thing/req/event/#"
    kafka_topic = mqttThingEvent
  }
]

common_topic = {
  client_connected = mqttClientConnected
  client_disconnected = mqttClientDisconnected
}
```

You need to manually create the kafka topuc first 
