import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.json.JSONObject;

import Config.RabbitMQConfig;
import Config.DBConfig;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Producer {
    private static final String QUEUE_NAME = "sales_queue";
    private static final String[] BRANCHES = {"branch1", "branch2"};

    public static void main(String[] args) {
        try (Connection rabbitConnection = RabbitMQConfig.getConnection();
             Channel channel = rabbitConnection.createChannel()) {

            // Declare a single queue for all branches
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println("[ ] Queue initialized: " + QUEUE_NAME);

            for (String branch : BRANCHES) {
                try (java.sql.Connection dbConnection = DBConfig.getBOConnection(branch)) {
                    String query = "SELECT * FROM sales WHERE synchronized = 0";

                    try (PreparedStatement statement = dbConnection.prepareStatement(query);
                         ResultSet resultSet = statement.executeQuery()) {

                        int rowsProcessed = 0;

                        while (resultSet.next()) {
                            JSONObject data = new JSONObject();
                            data.put("Branch", branch);  // Identifies which branch sent the data
                            data.put("SaleDate", resultSet.getDate("SaleDate"));
                            data.put("Region", resultSet.getString("Region"));
                            data.put("Product", resultSet.getString("Product"));
                            data.put("Qty", resultSet.getInt("Qty"));
                            data.put("Cost", resultSet.getDouble("Cost"));
                            data.put("Amt", resultSet.getDouble("Amt"));
                            data.put("Tax", resultSet.getDouble("Tax"));
                            data.put("Total", resultSet.getDouble("Total"));

                            // Publish message to the same queue
                            channel.basicPublish("", QUEUE_NAME, null, data.toString().getBytes());
                            System.out.println("[x] Sent from " + branch + ": " + data);

                            // Mark as synchronized in database
                            String updateQuery = "UPDATE sales SET synchronized = 1 WHERE SaleDate = ? AND Region = ? AND Product = ?";
                            try (PreparedStatement updateStatement = dbConnection.prepareStatement(updateQuery)) {
                                updateStatement.setDate(1, resultSet.getDate("SaleDate"));
                                updateStatement.setString(2, resultSet.getString("Region"));
                                updateStatement.setString(3, resultSet.getString("Product"));
                                int rowsUpdated = updateStatement.executeUpdate();
                                System.out.println("[ ] Updated " + rowsUpdated + " rows in " + branch);
                            }

                            rowsProcessed++;
                        }
                        System.out.println("[ ] Total Rows Sent from " + branch + ": " + rowsProcessed);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[ERROR] Producer failed!");
            e.printStackTrace();
        }
    }
}
