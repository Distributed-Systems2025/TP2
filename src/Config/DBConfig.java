package Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConfig {
    private static final String BO1_URL = "jdbc:mysql://localhost:3306/branch1";
    private static final String BO2_URL = "jdbc:mysql://localhost:3306/branch2";
    private static final String HO_URL = "jdbc:mysql://localhost:3306/head_office";
    private static final String USER = "root";
    private static final String PASSWORD = "MYSQLacc06*";

    public static Connection getBOConnection(String branch) throws SQLException {
        String dbUrl = branch.equalsIgnoreCase("branch1") ? BO1_URL : BO2_URL;
        return DriverManager.getConnection(dbUrl, USER, PASSWORD);
    }

    public static Connection getHOConnection() throws SQLException {
        return DriverManager.getConnection(HO_URL, USER, PASSWORD);
    }
}