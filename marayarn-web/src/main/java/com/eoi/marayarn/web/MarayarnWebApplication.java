package com.eoi.marayarn.web;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan("com.eoi.marayarn.web.mapper")
@SpringBootApplication
public class MarayarnWebApplication {

    public static void main(String[] args) {
        SpringApplication.run(MarayarnWebApplication.class, args);
    }

}
