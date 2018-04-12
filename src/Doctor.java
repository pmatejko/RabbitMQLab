import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Doctor {
    public static final String EXCHANGE_NAME = "exchange1";
    private Connection connection;
    private Channel channel;

    public Doctor() throws IOException, TimeoutException {
        // info
        System.out.println("Doctor");

        // connection & channel
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        // exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
    }

    public void startWorking() throws IOException {
        boolean working = true;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while (working) {
            // read msg
            System.out.println("Enter message like this:\n" +
                    "type-of-exam name-of-patient");
            String input = br.readLine();

            // break condition
            if ("exit".equals(input)) {
                br.close();
                break;
            }

            String[] splittedInput = input.split(" ");
            String key = splittedInput[0];
            String message = splittedInput[1];

            // publish
            channel.basicPublish(EXCHANGE_NAME, key, null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("Sent: " + message);
        }
    }

    public static void main(String[] argv) throws Exception {
        Doctor doctor = new Doctor();
        doctor.startWorking();
    }
}
