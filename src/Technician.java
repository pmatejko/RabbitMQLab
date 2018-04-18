import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Technician {
    private static final String TECHNICIAN = "Technician:";
    private final Connection connection;
    private final Channel channel;

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
        factory.setHost(Constants.LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(1);

        // exchange
        channel.exchangeDeclare(Constants.TECHNICIAN_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(Constants.DOCTOR_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(Constants.ADMIN_EXCHANGE, BuiltinExchangeType.FANOUT);

        // queue & bind
        for (String type : types) {
            channel.queueDeclare(type, false, false, false, null);
            channel.queueBind(type, Constants.TECHNICIAN_EXCHANGE, type);
        }

        String adminQueue = channel.queueDeclare().getQueue();
        channel.queueBind(adminQueue, Constants.ADMIN_EXCHANGE, "");

        // consumer (message handling)
        Consumer adminConsumer = new SimpleConsumer(channel);
        Consumer doctorConsumer = new SimpleConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties props, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, props, body);

                try {
                    Thread.sleep(1_000);

                    String response = getMessage().concat(" done");
                    channel.basicPublish(Constants.DOCTOR_EXCHANGE, props.getReplyTo(), null,
                            response.getBytes(StandardCharsets.UTF_8));

                    channel.basicAck(envelope.getDeliveryTag(), false);
                    System.out.println("Sent: " + response);
                } catch (InterruptedException e) {
                    System.err.println(e.toString().toUpperCase());
                }
            }
        };

        // start listening
        System.out.println("Waiting for messages...");
        for (String type : types) {
            channel.basicConsume(type, false, doctorConsumer);
        }
        channel.basicConsume(adminQueue, true, adminConsumer);
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
