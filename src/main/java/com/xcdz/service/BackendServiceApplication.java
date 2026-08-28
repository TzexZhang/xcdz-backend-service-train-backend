package com.xcdz.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Slf4j
@SpringBootApplication
public class BackendServiceApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(BackendServiceApplication.class,args);
        Environment env = context.getEnvironment();
        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            String port = env.getProperty("server.port");
            String path = env.getProperty("server.servlet.context-path");
            path = path == null ? "": path;
            log.info("\n---------------------------------------------------------------------------\n\t" +
                    "Application BackendServiceApplication is Running!\n\t" +
                    "Local:\t\thttp://localhost:" + port + path + "/\n\t" +
                    "External:\thttp://" + ip + ":" + port + path + "/\n\t" +
                    "docs:\t\thttp://" + ip + ":" + port + path + "/doc.html\n\t" +
                    "-------------------------------------------------------------------------------");
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
