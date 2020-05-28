package spark.controller;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;
import spark.Application;

import java.util.List;


@RestController
@CrossOrigin("http://localhost:8081")
public class WebController {
    private Logger logger;
    public WebController() {
        logger = Logger.getLogger(WebController.class);
        logger.setLevel(Level.INFO);
    }

    @GetMapping("/hello")
    public String hello() {
        return "Hello";
    }

    @GetMapping("/recommend")
    @ResponseBody
    public List<Tuple2<String, Double>> recommend(@RequestParam("user") int user) {
        logger.info("GET: /recommend?user=" + user);
        return Application.rh.recommend(user, 10);
    }

    @GetMapping("/count")
    @ResponseBody
    public long count(@RequestParam("user") int user) {
        logger.info("GET: /count?user=" + user);
        return Application.rh.count(user);
    }

    @GetMapping("/history")
    @ResponseBody
    public List<Tuple2<String, Integer>> history(@RequestParam("user") int user) {
        logger.info("GET: /history?user=" + user);
        return Application.rh.history(user);
    }
}