import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Doctor {
    private static final String DOCTOR = "Doctor";
    private final Connection connection;
    private final Channel channel;
    private final AMQP.BasicProperties props;

    public Doctor() throws IOException, TimeoutException {
        // info
        System.out.println(DOCTOR);

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Constants.LOCALHOST);
        connection = factory.newConnection();
        channel = connection.createChannel();

        // exchange
        channel.exchangeDeclare(Constants.TECHNICIAN_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(Constants.DOCTOR_EXCHANGE, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(Constants.ADMIN_EXCHANGE, BuiltinExchangeType.FANOUT);

        // queue & bind
        String adminQueue = channel.queueDeclare().getQueue();
        channel.queueBind(adminQueue, Constants.ADMIN_EXCHANGE, "");

        String technicianQueue = channel.queueDeclare().getQueue();
        channel.queueBind(technicianQueue, Constants.DOCTOR_EXCHANGE, technicianQueue);
        props = new AMQP.BasicProperties(null, null, null, null,
                null, null, technicianQueue, null, null, null,
                null, null, null, null);

        // consumer (message handling)
        Consumer consumer = new SimpleConsumer(channel);
        channel.basicConsume(technicianQueue, true, consumer);
        channel.basicConsume(adminQueue, true, consumer);
    }

    public void startWorking() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            // read msg
            System.out.println("Enter message like this:\n" +
                    "name-of-patient type-of-exam");
            String input = br.readLine();

            // break condition
            if ("exit".equals(input)) {
                br.close();
                break;
            }

            try {
                // publish
                String key = input.split(" ")[1];
                channel.basicPublish(Constants.TECHNICIAN_EXCHANGE, key, props, input.getBytes(StandardCharsets.UTF_8));
                System.out.println("Sent: " + input);
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("Wrong message format");
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Doctor doctor = new Doctor();
        doctor.startWorking();
    }
}
