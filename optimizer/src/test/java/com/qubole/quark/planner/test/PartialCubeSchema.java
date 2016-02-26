package com.qubole.quark.planner.test;

import com.google.common.collect.ImmutableList;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.QuarkCube;
import com.qubole.quark.sql.QueryContext;

import java.util.List;

/**
 * Created by rajatv on 2/21/16.
 */
public class PartialCubeSchema extends MetadataSchema {
  public final String filter;

  PartialCubeSchema(String filter) {
    this.filter = filter;
  }

  public QuarkCube storeSalesCubePartial() {
    final List<QuarkCube.Measure> measures = new ImmutableList.Builder<QuarkCube.Measure>()
        .add(new QuarkCube.Measure("sum", "ss_ext_sales_price".toUpperCase(),
            "sum_extended_sales_price".toUpperCase()))
        .add(new QuarkCube.Measure("sum", "ss_sales_price".toUpperCase(),
            "sum_sales_price".toUpperCase()))
        .build();

    final ImmutableList<QuarkCube.Dimension> dimensions = new ImmutableList.Builder<QuarkCube.Dimension>()
        .add(QuarkCube.Dimension.builder("I_ITEM_ID", "", "I", "I_ITEM_ID",
            "I_ITEM_ID", 0).build())
        .add(QuarkCube.Dimension.builder("C_CUSTOMER_ID", "", "C", "C_CUSTOMER_ID",
            "C_CUSTOMER_ID", 1).build())
        .add(QuarkCube.Dimension.builder("D_YEAR", "", "DD", "D_YEAR", "D_YEAR", 2).setMandatory(true)
            .build())
        .add(QuarkCube.Dimension.builder("D_MOY", "", "DD", "D_MOY", "D_MOY", 3).setMandatory(true)
            .build())
        .add(QuarkCube.Dimension.builder("D_DOM", "", "DD", "D_DOM", "D_DOM", 4).setMandatory(true)
            .build())
        .add(QuarkCube.Dimension.builder("CD_GENDER", "", "CD", "CD_GENDER",
            "CD_GENDER", 5).build())
        .add(QuarkCube.Dimension.builder("CD_MARITAL_STATUS", "", "CD", "CD_MARITAL_STATUS",
            "CD_MARITAL_STATUS", 6).build())
        .add(QuarkCube.Dimension.builder("CD_EDUCATION_STATUS", "", "CD", "CD_EDUCATION_STATUS",
            "CD_EDUCATION_STATUS", 7).build())
        .build();

    final QuarkCube count_fact = new QuarkCube("store_sales_cube_partial",
        "select 1 from tpcds.store_sales as ss " +
            "join tpcds.item as i on ss.ss_item_sk = i.i_item_sk " +
            "join tpcds.customer as c on ss.ss_customer_sk = c.c_customer_sk " +
            "join tpcds.date_dim as dd on ss.ss_sold_date_sk = dd.d_date_sk " +
            "join tpcds.customer_demographics cd on ss.ss_cdemo_sk = cd.cd_demo_sk " +
            this.filter,
        measures, dimensions, ImmutableList.of("TPCDS", "STORE_SALES_CUBE_PARTIAL"),
        "GROUPING_ID");

    return count_fact;
  }

  @Override
  public void initialize(QueryContext queryContext) throws QuarkException {
    this.views = ImmutableList.of();
    this.cubes = ImmutableList.of(storeSalesCubePartial());
    super.initialize(queryContext);
  }
}
