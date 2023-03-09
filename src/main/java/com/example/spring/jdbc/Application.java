package com.example.spring.jdbc;

import com.example.spring.jdbc.config.AppConfig;
import com.example.spring.jdbc.service.TableService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class Application {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    val context =
        new AnnotationConfigApplicationContext(AppConfig.class);
    val tableService = context.getBean(TableService.class);

    val configFilePath = args[0];
    val configContent = Files.readString(Paths.get(configFilePath));

    log.info("Start reading configuration file...");

    int numberOfTables = Integer.parseInt(
        Objects.requireNonNull(getConfigValue(configContent, "tables.number")));
    int numberOfColumns = Integer.parseInt(
        Objects.requireNonNull(getConfigValue(configContent, "columns.number")));
    int numberOfConcurrentConnections = Integer.parseInt(
        Objects.requireNonNull(getConfigValue(configContent, "connections.number")));
    val columnTypes = getConfigList(configContent, "columns.types");
    val rowsCount = getConfigList(configContent, "rows.count").stream()
        .map(Long::parseLong)
        .toList();

    val tablesCreated = tableService.createTablesConcurrently(numberOfTables, numberOfColumns,
        columnTypes, numberOfConcurrentConnections);

    tableService.populateTablesConcurrently(tablesCreated, rowsCount, numberOfConcurrentConnections);

    context.close();
  }

  private static String getConfigValue(String configContent, String key) {
    String[] lines = configContent.split("\n");
    for (String line : lines) {
      String[] parts = line.trim().split("=");
      if (parts.length == 2 && parts[0].equals(key)) {
        return parts[1];
      }
    }
    return null;
  }

  private static List<String> getConfigList(String configContent, String key) {
    String[] lines = configContent.split("\n");
    for (String line : lines) {
      String[] parts = line.trim().split("=");
      if (parts.length == 2 && parts[0].equals(key)) {
        String[] values = parts[1].split(",");
        List<String> result = new ArrayList<>();
        for (String value : values) {
          result.add(value.trim());
        }
        return result;
      }
    }
    return Collections.emptyList();
  }

}
