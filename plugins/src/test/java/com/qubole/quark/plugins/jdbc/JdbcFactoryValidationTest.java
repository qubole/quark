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
  public void testValidProps() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "mysql");
    props.put("url", "jdbc:quark:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidType() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "Quark");
    props.put("url", "jdbc:quark:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeNull() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("url", "jdbc:quark:test");
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
    props.put("url", "jdbc:quark:test");
    props.put("username", "quark");
    props.put("password", "quark");
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPasswordNull() throws QuarkException {
    Map<String, Object> props = new HashMap<>();
    props.put("type", "Quark");
    props.put("url", "jdbc:quark:test");
    props.put("username", "quark");
    factory.create(props);
  }

}
