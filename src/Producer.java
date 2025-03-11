import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.json.JSONObject;

import Config.RabbitMQConfig;
import Config.DBConfig;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Producer {
    private static final String QUEUE_NAME = "sales_queue";

    public static void main(String[] args) {
        try (Connection rabbitConnection = RabbitMQConfig.getConnection();
             Channel channel = rabbitConnection.createChannel();
             java.sql.Connection dbConnection = DBConfig.getBOConnection("branch1")) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            String query = "SELECT * FROM sales WHERE synchronized = 0";
            PreparedStatement statement = dbConnection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                JSONObject data = new JSONObject();
                data.put("SaleDate", resultSet.getDate("SaleDate"));
                data.put("Region", resultSet.getString("Region"));
                data.put("Product", resultSet.getString("Product"));
                data.put("Qty", resultSet.getInt("Qty"));
                data.put("Cost", resultSet.getDouble("Cost"));
                data.put("Amt", resultSet.getDouble("Amt"));
                data.put("Tax", resultSet.getDouble("Tax"));
                data.put("Total", resultSet.getDouble("Total"));

                channel.basicPublish("", QUEUE_NAME, null, data.toString().getBytes());
                System.out.println("[x] Sent: " + data);

                // Mark as synchronized
                String updateQuery = "UPDATE sales SET synchronized = 0 WHERE SaleDate = ? AND Region = ? AND Product = ?";
                PreparedStatement updateStatement = dbConnection.prepareStatement(updateQuery);
                updateStatement.setDate(1, resultSet.getDate("SaleDate"));
                updateStatement.setString(2, resultSet.getString("Region"));
                updateStatement.setString(3, resultSet.getString("Product"));
                updateStatement.executeUpdate();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
