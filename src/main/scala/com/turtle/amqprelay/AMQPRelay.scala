
/*
 * Copyright 2009 Turtle Entertainment GmbH
 * Copyright 2009 Sebastian Latza <sel@turtle-entertainment.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.turtle.amqprelay

import scala.actors.Actor
import scala.actors.Actor._
import com.rabbitmq.client._
import net.lag.configgy._
import net.lag.logging._

case class AMQPMessage(tag: String, env: Envelope, props: AMQP.BasicProperties, body: Array[Byte], a: Actor)
case class AMQPAckMessage(deliveryTag: Long)
case class AMQPReconnect(i: Int)

trait Configuration {
  var config = {
    Configgy.configure("conf/relay.conf")
    val config = Configgy.config
    config.registerWithJmx("amqp_relay")
    config
  } 
}

trait Logging extends Configuration {
  var log = {
	Logger.get(this.getClass.getName)
  }
}

class AMQPConsumer(exchange_name: String) extends Actor with Configuration with Logging {
	log.info("Initializing relay for exchange %s", exchange_name)

	def connect = {
	   	log.debug("Connecting to %s", config.getString("master.host", "localhost"))
		val params = new ConnectionParameters
		params.setUsername(config.getString("master.username", "guest"))
		params.setPassword(config.getString("master.password", "guest"))
		params.setVirtualHost(config.getString("master.vhost", "/"))
		params.setRequestedHeartbeat(1)
		val factory = new ConnectionFactory(params)
		val connection = factory.newConnection(config.getString("master.host", "localhost"), config.getInt("master.port", 5672))
		log.info("Successfully connected consumer to %s", config.getString("master.host", "localhost"))
		val producer = new AMQPProducer(exchange_name)
		producer.start
		val channel = connection.createChannel
		channel.queueDeclare(exchange_name + "_relay" , true)
		channel.queueBind(exchange_name + "_relay", exchange_name, "#")
		channel.basicQos(25)
		val ref = this
		val consumer = new DefaultConsumer(channel) {
			override def handleDelivery(tag: String, env: Envelope, props: AMQP.BasicProperties, body: Array[Byte]) = {  
				producer ! AMQPMessage(tag, env, props, body, ref)
			}
		}	
		channel.basicConsume(exchange_name + "_relay", false, consumer)
		val listener = new ShutdownListener {
	   		override def shutdownCompleted(cause: ShutdownSignalException) {
	   		  log.error("Lost connection to %s", config.getString("master.host", "localhost"))
	   		  ref ! AMQPReconnect(0)
	   		}
	   	}
	   	connection.addShutdownListener(listener)
		channel
	}
	
 	var channel = connect
 	def act = {
	  loop {
		react {
		  case AMQPReconnect(i: Int) =>
		    channel = connect
		  case AMQPAckMessage(deliveryTag) =>
		    channel.basicAck(deliveryTag, false)
		  case _ => log.error("Unhandled Message.")
		}
	  }
	}
}

class AMQPProducer(exchange_name: String) extends Actor with Configuration with Logging {
	def connect = {
		log.debug("Connecting to %s", config.getString("remote.host", "localhost"))
		val params = new ConnectionParameters
		params.setUsername(config.getString("remote.username", "guest"))
		params.setPassword(config.getString("remote.password", "guest"))
		params.setVirtualHost(config.getString("remote.vhost", "/"))
		params.setRequestedHeartbeat(1)
		val factory = new ConnectionFactory(params)
		val connection = factory.newConnection(config.getString("remote.host", "localhost"), config.getInt("remote.port", 5672))
		log.info("Successfully connected producer to %s", config.getString("remote.host", "localhost"))
		val channel = connection.createChannel
		val ref = this
  		val listener = new ShutdownListener {
	   		override def shutdownCompleted(cause: ShutdownSignalException) {
	   		  log.error("Lost connection to %s", config.getString("remote.host", "localhost"))
	   		  ref ! AMQPReconnect(0)
	   		}
	   	}
	   	connection.addShutdownListener(listener)
		channel
	}
 
	var channel = connect
	
	def act = {
	  loop {
		react {
		  case AMQPMessage(tag, env, props, body, a) =>
		    log.debug("Relaying message %s", new String(body))
	      	channel.basicPublish(exchange_name, env.getRoutingKey, props, body)
	      	a ! AMQPAckMessage(env.getDeliveryTag)
		  case AMQPReconnect(i: Int) =>
	      	try {
	      		channel = connect
	      	} catch {
		  		case e: Exception => 
		  		  Thread.sleep(1000)
		  		  self ! AMQPReconnect(i + 1)     		
	      	}
		  case _ => log.error("Unhandled Message.")
		}
	  }
	}
}

object AMQPRelay extends Configuration with Logging {
  def main(args: Array[String]) { 
	log.info("Starting AMQPRelay..")
	config.getList("relayed_exchanges").foreach(exchange => new AMQPConsumer(exchange).start) 
  }
}

