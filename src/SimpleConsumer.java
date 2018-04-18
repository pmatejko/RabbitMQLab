import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimpleConsumer extends DefaultConsumer {
    private String message;

    public SimpleConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties props, byte[] body) throws IOException {
        message = new String(body, StandardCharsets.UTF_8);
        System.err.println("Received: " + message);
    }

    public String getMessage() {
        return message;
    }
}