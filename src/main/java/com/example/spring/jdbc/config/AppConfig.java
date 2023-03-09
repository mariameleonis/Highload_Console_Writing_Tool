package com.example.spring.jdbc.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan("com.example.spring.jdbc")
public class AppConfig {

  @Value("${spring.datasource.driver-class-name}")
  private String dbDriverClassName;

  @Value("${spring.datasource.maximum-pool-size}")
  private int dbMaximumPoolSize;

  public static final String JDBC_URL = System.getenv("JDBC_URL");
  public static final String USER = System.getenv("JDBC_USER");
  public static final String PASSWORD = System.getenv("JDBC_PASSWORD");

  @Bean
  public DataSource dataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(JDBC_URL);
    config.setUsername(USER);
    config.setPassword(PASSWORD);
    config.setDriverClassName(dbDriverClassName);
    config.setMaximumPoolSize(dbMaximumPoolSize);

    return new HikariDataSource(config);
  }

  @Bean
  public JdbcTemplate jdbcTemplate(DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

}
