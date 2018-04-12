import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Admin {
    private static final String ADMIN = "Admin";
    private static final String LOCALHOST = "localhost";
    private static final String EXCHANGE_NAME = "exchange1";
    private Connection connection;
    private Channel channel;

    public Admin() throws IOException, TimeoutException {
        // info
        System.out.println(ADMIN);

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(1);

        // exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // queue & bind
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "#");

        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                System.out.println("Received: " + message);
            }
        };

        // start listening
        System.out.println("Waiting for messages...");
        channel.basicConsume(queueName, true, consumer);
    }

    public void writeMessage(String msg) throws IOException {
        channel.basicPublish(EXCHANGE_NAME, "*", null, msg.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        Admin admin = new Admin();

        Thread.sleep(5000);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));


        while (true) {
            String msg = br.readLine();
            admin.writeMessage(msg);
        }

    }
}
