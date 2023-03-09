package com.example.spring.jdbc.service;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class TableService {

  public static final Random RANDOM = new Random();
  private final JdbcTemplate jdbcTemplate;

  public List<String> createTablesConcurrently(int numberOfTables, int numberOfColumns,
      List<String> columnTypes, int numberOfConcurrentConnections)
      throws ExecutionException, InterruptedException {
    val executor = Executors.newFixedThreadPool(numberOfConcurrentConnections);
    List<Future<String>> futures = new ArrayList<>();

    for (int i = 0; i < numberOfTables; i++) {
      futures.add(executor.submit(() -> createTable(numberOfColumns, columnTypes)));
    }

    log.info("Start creating tables...");

    List<String> results = new ArrayList<>();
    for (Future<String> future : futures) {
      results.add(future.get());
    }

    executor.shutdown();

    log.info("Tables " + results + " created successfully!");
    return results;
  }

  public String createTable(int numberOfColumns, List<String> columnTypes) {
    val tableName = "table_" + UUID.randomUUID().toString().replace("-", "");
    val sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(tableName);
    sb.append(" (");

    for (int i = 0; i < numberOfColumns; i++) {
      String columnName = "column_" + i;
      String columnType = columnTypes.get(RANDOM.nextInt(columnTypes.size()));
      sb.append(columnName);
      sb.append(" ");
      sb.append(columnType);
      sb.append(",");
    }

    sb.deleteCharAt(sb.length() - 1);
    sb.append(")");

    log.info("Start executing query: " + sb);

    jdbcTemplate.execute(sb.toString());

    log.info("Table " + tableName + " created successfully!");

    return tableName;
  }

  public void populateTablesConcurrently(List<String> tableNames, List<Long> rowsCount,
      int numberOfConcurrentConnections) throws ExecutionException, InterruptedException {
    val executor = Executors.newFixedThreadPool(numberOfConcurrentConnections);
    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < tableNames.size(); i++) {
      val tableName = tableNames.get(i);
      val rowCount = rowsCount.get(i);
      futures.add(executor.submit(() -> populateTable(tableName, rowCount)));
    }

    log.info("Start populating tables...");

    for (Future<?> future : futures) {
      future.get();
    }

    log.info("Tables populated successfully!");

    executor.shutdown();

  }

  private void populateTable(String tableName, Long rowCount) {
    val columnNames = getTableColumnNames(tableName);
    List<Object[]> batchArgs = new ArrayList<>();

    for (int i = 0; i < rowCount; i++) {
      Object[] row = new Object[columnNames.length];
      for (int j = 0; j < columnNames.length; j++) {
        String columnType = getTableColumnType(tableName, columnNames[j]);
        row[j] = getRandomValueForColumnType(columnType, RANDOM);
      }
      batchArgs.add(row);
    }

    jdbcTemplate.batchUpdate("INSERT INTO " + tableName + " VALUES (" + String.join(",", Collections.nCopies(columnNames.length, "?")) + ")", batchArgs);
  }

  private String[] getTableColumnNames(String tableName) {
    List<Map<String, Object>> columns = jdbcTemplate.queryForList("SELECT column_name FROM information_schema.columns WHERE table_name = ?", tableName);
    String[] columnNames = new String[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      columnNames[i] = (String) columns.get(i).get("column_name");
    }
    return columnNames;
  }

  private String getTableColumnType(String tableName, String columnName) {
    List<Map<String, Object>> columns = jdbcTemplate.queryForList("SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?", tableName, columnName);
    return (String) columns.get(0).get("data_type");
  }

  private Object getRandomValueForColumnType(String columnType, Random random) {
    switch (columnType) {
      case "integer":
        return random.nextInt();
      case "bigint":
        return random.nextLong();
      case "real":
        return random.nextFloat();
      case "double precision":
        return random.nextDouble();
      case "boolean":
        return random.nextBoolean();
      case "character varying":
      case "text":
        return getRandomString(10, random);
      case "date":
        return new Date(System.currentTimeMillis() - random.nextInt(1000000000));
      case "timestamp without time zone":
        return new Timestamp(System.currentTimeMillis() - random.nextInt(1000000000));
      default:
        throw new IllegalArgumentException("Unsupported column type: " + columnType);
    }
  }

  private String getRandomString(int length, Random random) {
    String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sb.append(characters.charAt(random.nextInt(characters.length())));
    }
    return sb.toString();
  }

}
