import Config.DBConfig;
import Config.RabbitMQConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;
import com.rabbitmq.client.AMQP;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;

public class Consumer {
    private static final String QUEUE_NAME = "sales_queue";

    public static void main(String[] args) {
        try {
            System.out.println("[ ] Connecting to RabbitMQ...");
            Connection rabbitConnection = RabbitMQConfig.getConnection();
            Channel channel = rabbitConnection.createChannel();

            // Ensure the queue exists
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println("[ ] Listening to queue: " + QUEUE_NAME);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();

                System.out.println("[x] Received: " + message);

                JSONObject data = new JSONObject(message);
                String branch = data.getString("Branch"); // Identify the branch

                try (java.sql.Connection dbConnection = DBConfig.getHOConnection()) {
                    String insertQuery = """
                        INSERT INTO sales (SaleDate, Region, Product, Qty, Cost, Amt, Tax, Total)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE 
                            Qty = VALUES(Qty),
                            Cost = VALUES(Cost),
                            Amt = VALUES(Amt),
                            Tax = VALUES(Tax),
                            Total = VALUES(Total);
                        """;

                    System.out.println("[ ] Preparing SQL Insert from " + branch + "...");
                    try (PreparedStatement insertStatement = dbConnection.prepareStatement(insertQuery)) {
                        insertStatement.setDate(1, java.sql.Date.valueOf(data.getString("SaleDate")));
                        insertStatement.setString(2, data.getString("Region"));
                        insertStatement.setString(3, data.getString("Product"));
                        insertStatement.setInt(4, data.getInt("Qty"));
                        insertStatement.setDouble(5, data.getDouble("Cost"));
                        insertStatement.setDouble(6, data.getDouble("Amt"));
                        insertStatement.setDouble(7, data.getDouble("Tax"));
                        insertStatement.setDouble(8, data.getDouble("Total"));

                        int rowsAffected = insertStatement.executeUpdate();
                        System.out.println("[x] Inserted/Updated " + rowsAffected + " rows from " + branch);

                        channel.basicAck(deliveryTag,false);
                        System.out.println("[ACK] Message acknowledged (Removed from queue)");
                    }
                } catch (Exception e) {
                    System.out.println("[ERROR] Failed to insert from " + branch);
                    e.printStackTrace();

                    channel.basicNack(deliveryTag, false, true);
                    System.out.println("[NACK] Message rejected and requeued");
                }
            };

            // Start listening to the queue
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
            System.out.println("[ ] Consumer is running and listening for messages...");

        } catch (Exception e) {
            System.out.println("[ERROR] Consumer failed to start!");
            e.printStackTrace();
        }
    }
}
