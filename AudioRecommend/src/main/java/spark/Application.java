package spark;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;
import spark.Service.Recommend;

import java.io.IOException;
import java.util.List;

@SpringBootApplication
@EnableAutoConfiguration
public class Application {
    public static Recommend rh = new Recommend().loadData().loadModel("lab5/model");
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        System.out.println("---------------------------called---------------------------");
    }
}
