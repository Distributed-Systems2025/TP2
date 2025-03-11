import Config.DBConfig;
import Config.RabbitMQConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;

public class Consumer {
    private static final String QUEUE_NAME = "sales_queue";

    public static void main(String[] args) {
        try {
            System.out.println("[] Connecting to RabbitMQ...");
            Connection rabbitConnection = RabbitMQConfig.getConnection();
            Channel channel = rabbitConnection.createChannel();

            // Ensure the queue exists
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println("[] Connected to RabbitMQ. Waiting for messages...");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("[x] Received: " + message); // CONFIRM MESSAGE RECEIVED

                JSONObject data = new JSONObject(message);

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

                    System.out.println("[] Preparing SQL Insert Statement...");
                    PreparedStatement insertStatement = dbConnection.prepareStatement(insertQuery);
                    insertStatement.setDate(1, java.sql.Date.valueOf(data.getString("SaleDate")));
                    insertStatement.setString(2, data.getString("Region"));
                    insertStatement.setString(3, data.getString("Product"));
                    insertStatement.setInt(4, data.getInt("Qty"));
                    insertStatement.setDouble(5, data.getDouble("Cost"));
                    insertStatement.setDouble(6, data.getDouble("Amt"));
                    insertStatement.setDouble(7, data.getDouble("Tax"));
                    insertStatement.setDouble(8, data.getDouble("Total"));

                    int rowsAffected = insertStatement.executeUpdate();
                    System.out.println("[x] Rows inserted/updated: " + rowsAffected);
                } catch (Exception e) {
                    System.out.println("[ERROR] Failed to insert into MySQL!");
                    e.printStackTrace();
                }
            };

            // Start listening to the queue
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
            System.out.println("[DEBUG] Consumer is running and listening for messages...");

        } catch (Exception e) {
            System.out.println("[ERROR] Consumer failed to start!");
            e.printStackTrace();
        }
    }
}
