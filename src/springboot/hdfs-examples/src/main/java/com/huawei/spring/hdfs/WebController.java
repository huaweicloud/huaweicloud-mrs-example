package com.huawei.spring.hdfs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class WebController {

    @Autowired
    CustomerHDFSTemplate hdfsTemplate;

    private String basePath = "/tmp/examples";

    @GetMapping("/save")
    public String save(@RequestParam("fileName") String fileName) {
        try {
            hdfsTemplate.mkdir(basePath);
            hdfsTemplate.write(basePath, fileName);
        } catch (IOException e) {
            return e.toString();
        }
        return "Done";
    }

    @GetMapping("/remove")
    public String remove(@RequestParam("fileName") String fileName) {
        try {
            hdfsTemplate.remove(basePath + "/" + fileName);
        } catch (IOException e) {
            return e.toString();
        }
        return "Done";
    }

    @GetMapping("/read")
    public String read(@RequestParam("fileName") String fileName) {
        try {
            return hdfsTemplate.read(basePath, fileName);
        } catch (IOException e) {
            return e.toString();
        }
    }

}
