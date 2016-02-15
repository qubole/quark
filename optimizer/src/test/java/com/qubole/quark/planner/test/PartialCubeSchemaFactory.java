package com.qubole.quark.planner.test;

import com.google.common.collect.ImmutableList;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.TestFactory;

import java.util.List;
import java.util.Properties;

/**
 * Created by rajatv on 2/21/16.
 */
public class PartialCubeSchemaFactory implements TestFactory {
  public List<QuarkSchema> create(Properties info) {
    Tpcds tpcds = new Tpcds("TPCDS");
    PartialCubeSchema cubeSchema = new PartialCubeSchema(info.getProperty("filter"));
    return new ImmutableList.Builder<QuarkSchema>()
        .add(tpcds)
        .add(cubeSchema).build();
  }
}
