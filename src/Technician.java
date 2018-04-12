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

public class Technician {
    private static final String TECHNICIAN = "Technician:";
    private static final String LOCALHOST = "localhost";
    private static final String EXCHANGE_NAME = "exchange1";
    private Connection connection;
    private Channel channel;

    public Technician(String... types) throws IOException, TimeoutException {
        // info
        System.out.print(TECHNICIAN);
        for (String type : types) {
            System.out.print(" ");
            System.out.print(type);
        }
        System.out.print("\n");

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(1);

        // exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // queue & bind
        for (String type : types) {
            channel.queueDeclare(type, false, false, false, null);
            channel.queueBind(type, EXCHANGE_NAME, type);
        }

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
        for (String type : types) {
            channel.basicConsume(type, true, consumer);
        }
    }

    public static void main(String[] argv) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter first type of examination: ");
        String first = br.readLine();
        System.out.println("Enter second type of examination: ");
        String second = br.readLine();
        br.close();

        Technician technician = new Technician(first, second);
    }
}
