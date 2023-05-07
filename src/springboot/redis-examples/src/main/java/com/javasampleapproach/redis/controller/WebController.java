package com.javasampleapproach.redis.controller;


import com.javasampleapproach.redis.util.CustomerRedisTemplateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class WebController {

    @Autowired
    CustomerRedisTemplateUtil customerRepository;

    @GetMapping("/save")
    public String save() {
        customerRepository.save("1", "a");
        customerRepository.save("2", "b");
        customerRepository.save("3", "c");
        return "Done";
    }

    @GetMapping("/find")
    public String findById(@RequestParam("id") String id) {
        return customerRepository.find(id);
    }

}
