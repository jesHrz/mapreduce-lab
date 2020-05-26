package spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import spark.service.Recommend;

@SpringBootApplication
//@EnableAutoConfiguration
public class Application {
    public static Recommend rh = new Recommend().loadData().loadModel("lab5/model");
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        System.out.println("---------------------------called---------------------------");
    }
}
