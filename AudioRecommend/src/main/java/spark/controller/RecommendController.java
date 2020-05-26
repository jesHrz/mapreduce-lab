package spark.controller;

import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import scala.Tuple2;
import spark.Application;

import java.util.List;


@Controller
public class RecommendController {
    private Logger logger = Logger.getLogger(RecommendController.class);

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    public ResponseEntity<String> hello() {
        return new ResponseEntity<>("233333", HttpStatus.OK);
    }

    @GetMapping(value = "/recommend/{user}")
    public ResponseEntity<List<Tuple2<String, Double>>> recommend(@PathVariable int user) {
        logger.info("GET: /recommend/" + user);
        return new ResponseEntity<>(Application.rh.recommend(user, 10), HttpStatus.OK);
    }
}