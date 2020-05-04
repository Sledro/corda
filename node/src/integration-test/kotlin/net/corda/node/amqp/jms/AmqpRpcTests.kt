package net.corda.node.amqp.jms

import net.corda.core.identity.CordaX500Name
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.coretesting.internal.configureTestSSL
import net.corda.nodeapi.RPCApi
import net.corda.nodeapi.internal.config.MutualSslConfiguration
import net.corda.services.messaging.SimpleMQClient
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.driver
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.jms.client.ActiveMQQueue
import org.apache.qpid.jms.JmsConnectionFactory
import org.junit.Test
import java.lang.Exception
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.jms.CompletionListener
import javax.jms.ConnectionFactory
import javax.jms.Message
import javax.jms.Session
import kotlin.test.assertTrue

class AmqpRpcTests {

    companion object {
        private val logger = contextLogger()

        const val TAG_FIELD_NAME = "tag" // RPCApi.TAG_FIELD_NAME
        const val RPC_REQUEST = 0// RPCApi.ClientToServer.Tag.RPC_REQUEST
    }

    @Test(timeout=300_000)
    fun `RPC over AMQP`() {
        driver(DriverParameters(startNodesInProcess = true, notarySpecs = emptyList())) {
            val node = startNode().get()
            val connectionFactory: ConnectionFactory = JmsConnectionFactory("amqp://${node.rpcAddress}")
            val rpcUser = node.rpcUsers.first()
            connectionFactory.createConnection(rpcUser.username, rpcUser.password).use { connection ->

                // Create a session
                val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

                val sender = session.createProducer(ActiveMQQueue(RPCApi.RPC_SERVER_QUEUE_NAME))

                /*val serialisedArguments = emptyList<Unit>().serialize(context = AMQP_RPC_CLIENT_CONTEXT)
                val request = RPCApi.ClientToServer.RpcRequest(
                        SimpleString("foo"),
                        "currentNodeTime",
                        serialisedArguments,
                        Trace.InvocationId(),
                        sessionId
                )*/

                val message = session.createBytesMessage()
                message.setIntProperty(TAG_FIELD_NAME, RPC_REQUEST )
                //val message = session.createTextMessage("Hello, World!")

                // Start connection
                connection.start()

                val latch = CountDownLatch(1)

                sender.send(message, object : CompletionListener {
                    override fun onException(message: Message, exception: Exception) {
                        logger.error(message.toString(), exception)
                    }

                    override fun onCompletion(message: Message) {
                        logger.info("Message successfully sent: $message")
                        latch.countDown()
                    }
                })

                assertTrue(latch.await(10, TimeUnit.SECONDS))
                logger.info("Obtaining response")
                /*
                val receiveQueue = session.createQueue("")
                val consumer: MessageConsumer = session.createConsumer(receiveQueue)

                // Step 7. receive the simple message
                /*val m = */consumer.receive(5000) as BytesMessage
                */
            }
        }
    }

    private fun loginToRPCAndGetClientQueue(nodeHandle: NodeHandle): String {
        val rpcUser = nodeHandle.rpcUsers.first()
        val client = clientTo(nodeHandle.rpcAddress)
        client.start(rpcUser.username, rpcUser.password, false)
        val clientQueueQuery = SimpleString("${RPCApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.${rpcUser.username}.*")
        return client.session.addressQuery(clientQueueQuery).queueNames.single().toString()
    }

    private fun clientTo(target: NetworkHostAndPort, sslConfiguration: MutualSslConfiguration? = configureTestSSL(CordaX500Name("MegaCorp", "London", "GB"))): SimpleMQClient {
        return SimpleMQClient(target, sslConfiguration)
    }
}