package com.qubole.quark.plugins.jdbc;

import com.qubole.quark.QuarkException;
import com.qubole.quark.plugins.qubole.QuboleFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by amoghm on 12/30/15.
 */
public class QuboleFactoryValidationTest {

  @Test
  public void testValidHive() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("type", "hive");
    props.put("endpoint", "jdbc:quark:fat:json:test");
    props.put("token", "quark");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidType() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("type", "h4");
    props.put("endpoint", "jdbc:quark:fat:json:test");
    props.put("token", "quark");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTypeNull() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("endpoint", "jdbc:quark:fat:json:test");
    props.put("token", "quark");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEndpointNull() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("type", "h2");
    props.put("token", "quark");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTokenNull() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("type", "h2");
    props.put("endpoint", "jdbc:quark:fat:json:test");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }

  @Test
  public void testValidDbTap() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("type", "dbtap");
    props.put("endpoint", "jdbc:quark:fat:json:test");
    props.put("token", "quark");
    props.put("dbtapid", "7676");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }

  @Test(expected = QuarkException.class)
  public void testDbTapIdNull() throws QuarkException{
    Map<String, Object> props = new HashMap<>();
    props.put("type", "dbtap");
    props.put("endpoint", "jdbc:quark:fat:test");
    props.put("token", "quark");
    QuboleFactory factory = new QuboleFactory();
    factory.create(props);
  }
}
