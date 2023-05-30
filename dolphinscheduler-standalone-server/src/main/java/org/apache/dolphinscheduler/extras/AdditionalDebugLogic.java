package org.apache.dolphinscheduler.extras;

import lombok.SneakyThrows;
import org.h2.tools.Server;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;

@Component
public class AdditionalDebugLogic {

    @Autowired
    private DataSource dataSource;

    @PostConstruct
    @SneakyThrows
    public void postConstruct() {
        startH2InspectionServerAsync();
    }

    private void startH2InspectionServerAsync() {
        new Thread(() -> {
            try {
                log("Starting H2 inspection server");
                Connection connection = dataSource.getConnection();
                Server.startWebServer(connection);
                log("H2 inspection server ended");
            } catch (Exception e) {
                throw new RuntimeException("Failed to start H2 inspection server", e);
            }
        }).start();
    }

    public static void log(Object object) {
        System.err.printf("[MISAKA] %s%n", object);
    }

    public static void log(String template, Object... objects) {
        log(String.format(template, objects));
    }
}
