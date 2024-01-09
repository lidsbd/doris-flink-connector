package test;

import java.sql.*;

public class TestConnectionMetaData {
    public static void main(String[] args) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu:3306", "root", "root");
        DatabaseMetaData metaData = connection.getMetaData();
        System.out.println(metaData);

        ResultSet tables = metaData.getTables(connection.getCatalog(), null, "%",new String[] {"TABLE"});
        while (tables.next()) {
            System.out.println("tables = " + tables.next());
        }
    }
}
