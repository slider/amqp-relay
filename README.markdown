AMQPRelay
=========
A simple service to relay all messages from a topic exchange to another RabbitMQ broker instance. Think of Shovel, just in Scala.

## Quickstart
You need Scala and simple-build-tool to get started: 
<pre><code>
git clone git://github.com/slider/amqp-relay.git
cd amqp-relay
sbt compile
sbt run
</code></pre>

## Configuration
Configuration is done via conf/relay.conf, basic logging output goes to log/relay.log

## How does it work?

AMQPRelay creates a local buffer queue for every configured topic exchange, subscribes to it, and publishes incoming messages to a remote RabbitMQ instance, while preserving all message attributes and content. This might come in handy if you want to replicate your eventstream via WAN to another DC/EC2.

## License

AMQPRelay is licensed under the Apache 2 license (included).

