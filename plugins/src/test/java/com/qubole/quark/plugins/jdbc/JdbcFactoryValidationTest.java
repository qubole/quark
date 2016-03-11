package com.qubole.quark.plugins.jdbc;

import com.qubole.quark.QuarkException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by amoghm on 12/30/15.
 */
public class JdbcFactoryValidationTest {
  JdbcFactory factory = new JdbcFactory();

  @Test
  public void testGenericDb() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "GENERIC");
    props.put("catalogSql", "select table_schema, "
        + "table_name, column_name, data_type from "
        + "information_schema.columns order by "
        + "table_schema, table_name, ordinal_position;");
    props.put("productName", "hive");
    props.put("isCaseSensitive", "true");
    props.put("defaultSchema", "genericDbTest");
    props.put("url", "jdbc:quark:fat:json:genericDbTest");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test
  public void testValidProps() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "mysql");
    props.put("url", "jdbc:quark:fat:json:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidType() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "Quark");
    props.put("url", "jdbc:quark:fat:json:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeNull() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("url", "jdbc:quark:fat:json:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUrlNull() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "Quark");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUsernameNull() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "Quark");
    props.put("url", "jdbc:quark:fat:json:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPasswordNull() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "Quark");
    props.put("url", "jdbc:quark:fat:json:test");
    props.put("username", "quark");
    factory.create(props);
  }

}
