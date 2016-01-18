package com.coyotesong.coursera.cloud.configuration;

import javax.sql.DataSource;

import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.sun.xml.bind.v2.TODO;

/**
 * Repository configuration. We can use JPA during development and testing
 * and Cassandra in production.
 * 
 * {@link TODO} - use 'profile' to specify dev vs. production
 *
 * @author bgiles
 */
@Configuration
@EntityScan(basePackages = { "com.coyotesong.coursera.cloud.domain" })
@EnableJpaRepositories(basePackages = { "com.coyotesong.coursera.cloud.repository" })
@EnableTransactionManagement
public class RepositoryConfiguration {
    private EmbeddedDatabase db;
    
    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        db = builder.setType(EmbeddedDatabaseType.H2).build();
        return db;
    }
    
    // on exit db.shutdown()
}
