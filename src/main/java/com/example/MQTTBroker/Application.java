package com.example.MQTTBroker;

import com.example.MQTTBroker.bootstrap.Server;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;

@MapperScan("com.example.MQTTBroker.dao.mapper")
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication app=new SpringApplication(Application.class);
        app.run(args);
        new Server().startServer();
    }
}
