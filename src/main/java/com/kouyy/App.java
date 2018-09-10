package com.kouyy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages =
        {
                "com.kouyy"
        })
@SpringBootApplication
public class App

{
    public static void main( String[] args ) {
        SpringApplication.run(App.class, args);
        System.exit(0);
    }
}
