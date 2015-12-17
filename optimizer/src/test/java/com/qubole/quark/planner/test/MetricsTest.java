/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.planner.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.QuarkColumn;
import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.QuarkTable;
import com.qubole.quark.planner.QuarkView;
import com.qubole.quark.planner.TestFactory;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;
import com.qubole.quark.sql.QueryContext;
import org.apache.calcite.schema.Table;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by rajatv on 11/30/15.
 */
public class MetricsTest {
  private static final Logger log = LoggerFactory.getLogger(QueryTest.class);
  private static Properties info;

  public static class MetricsSchema extends TestSchema {
    MetricsSchema(String name) {
      super(name);
    }

    @Override
    protected Map<String, Table> getTableMap() {
      final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

      QuarkTable metrics = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
          .add(new QuarkColumn("NAME", "string"))
          .add(new QuarkColumn("VALUE", "double"))
          .add(new QuarkColumn("QUERY_HISTS_ID", "int"))
          .add(new QuarkColumn("CREATED_AT", "timestamp"))
          .add(new QuarkColumn("CREATED_AT_DATE", "date")).build());

      builder.put("METRICS", metrics);

      QuarkTable metrics_s3 = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
          .add(new QuarkColumn("NAME", "string"))
          .add(new QuarkColumn("VALUE", "double"))
          .add(new QuarkColumn("QUERY_HISTS_ID", "int"))
          .add(new QuarkColumn("CREATED_AT", "timestamp"))
          .add(new QuarkColumn("CREATED_AT_DATE", "date")).build());

      builder.put("METRICS_S3", metrics_s3);

      return builder.build();
    }
  }

  public static class ViewSchema extends MetadataSchema {
    ViewSchema() {}

    @Override
    public void initialize(QueryContext queryContext) throws QuarkException {
      this.cubes = ImmutableList.of();
      ImmutableList.Builder<QuarkView> viewHolderBuilder =
          new ImmutableList.Builder<>();

      final ImmutableList<String> metricsSchema = ImmutableList.<String>of("METRICS_SCHEMA");
      viewHolderBuilder.add(new QuarkView("S3N_METRICS",
          "select * from metrics_schema.metrics where " +
              "name='FileSystemCounters.S3N_BYTES_READ' " +
              "or name='FileSystemCounters.S3N_BYTES_WRITTEN' or " +
              "name='FileSystemCounters.S3N_LARGE_READ_OPS' or " +
              "name='FileSystemCounters.S3N_READ_OPS' or " +
              "name='FileSystemCounters.S3N_WRITE_OPS' or " +
              "name='FileSystemCounters.S3_BYTES_READ' or " +
              "name='FileSystemCounters.S3_BYTES_WRITTEN' or " +
              "name='FileSystemCounters.S3_LARGE_READ_OPS' or " +
              "name='FileSystemCounters.S3_READ_OPS' or " +
              "name='FileSystemCounters.S3_WRITE_OPS'",
          "METRICS_S3", metricsSchema, ImmutableList.<String>of("METRICS_SCHEMA", "METRICS_S3")));
      this.views = viewHolderBuilder.build();
      super.initialize(queryContext);
    }
  }

  public static class SchemaFactory implements TestFactory {
    public List<QuarkSchema> create(Properties info) {
      return new ArrayList<QuarkSchema>() {{
        add(new MetricsSchema("metrics_schema".toUpperCase()));
        add(new ViewSchema());
      }};
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    info = new Properties();
    info.put("schemaFactory", "com.qubole.quark.planner.test.MetricsTest$SchemaFactory");
    info.put("materializationsEnabled", "true");

    info.put("defaultSchema", QuarkTestUtil.toJson("METRICS_SCHEMA"));
  }

  @Test
  public void testViewFilter() throws QuarkException {
    QuarkTestUtil.checkParsedSql(
        "select name, value from metrics_schema.metrics where name='FileSystemCounters.S3_READ_OPS'",
        info,
        "SELECT NAME, VALUE FROM METRICS_SCHEMA.METRICS_S3 WHERE NAME = 'FileSystemCounters" +
            ".S3_READ_OPS'");
  }

}
