import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class EmployeeFileReadExample {

    
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Employee File Read Example")
                .master("local[4]")
                .getOrCreate();

        
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("opt/data/employee.csv");

         System.out.println("Employee Data:");
        df.show();

        Dataset<Row> highSalary = df.filter(col("salary").gt(50000));

        System.out.println("Employees with salary > 50000:");
        highSalary.show();

        
        Dataset<Row> avgSalary = df.groupBy("department")
                .agg(avg("salary").alias("average_salary"));

        System.out.println("Average salary by department:");
        avgSalary.show();
        spark.stop();
    }


    public static void startServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        server.createContext("/trigger", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String response;

                try {
                    main(null); // Call your unchanged Spark logic
                    response = "Spark Job Executed Successfully";
                } catch (Exception e) {
                    response = "Error: " + e.getMessage();
                }

                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        });

        server.setExecutor(null);
        server.start();
        System.out.println("Server started at http://localhost:8080/trigger");
    }

    
    static {
        try {
            startServer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
