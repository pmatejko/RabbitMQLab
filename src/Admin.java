import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Admin {
    private static final String ADMIN = "Admin";
    private Connection connection;
    private Channel channel;

    public Admin() throws IOException, TimeoutException {
        // info
        System.out.println(ADMIN);

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
        String technicianQueue = channel.queueDeclare().getQueue();
        String doctorQueue = channel.queueDeclare().getQueue();
        channel.queueBind(technicianQueue, Constants.TECHNICIAN_EXCHANGE, "#");
        channel.queueBind(doctorQueue, Constants.DOCTOR_EXCHANGE, "#");
        List<String> queueNames = Arrays.asList(technicianQueue, doctorQueue);

        // consumer (message handling)
        Consumer consumer = new SimpleConsumer(channel);

        // start listening
        System.out.println("Waiting for messages...");
        for (String queueName : queueNames) {
            channel.basicConsume(queueName, true, consumer);
        }
    }

    public void writeMessage(String msg) throws IOException {
        channel.basicPublish(Constants.ADMIN_EXCHANGE, "", null, msg.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        Admin admin = new Admin();

        Thread.sleep(5000);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.println("Enter message: ");
            String msg = br.readLine();
            admin.writeMessage(msg);
            System.out.println("Sent: " + msg);
        }
    }
}
