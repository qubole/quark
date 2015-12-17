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
import com.qubole.quark.planner.QuarkColumn;
import com.qubole.quark.planner.QuarkTable;
import org.apache.calcite.schema.Table;

import java.util.Map;

/**
 * Created by rajatv on 6/22/15.
 */
public class Tpcds extends TestSchema {
  Tpcds(String name) {
    super(name);
  }

  @Override
  public Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    QuarkTable customer_demographics = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("cd_demo_sk", "int"))
        .add(new QuarkColumn("cd_gender", "string"))
        .add(new QuarkColumn("cd_marital_status", "string"))
        .add(new QuarkColumn("cd_education_status", "string"))
        .add(new QuarkColumn("cd_purchase_estimate", "int"))
        .add(new QuarkColumn("cd_credit_rating", "string"))
        .add(new QuarkColumn("cd_dep_count", "int"))
        .add(new QuarkColumn("cd_dep_employed_count", "int"))
        .add(new QuarkColumn("cd_dep_college_count", "int")).build()
    );

    builder.put("CUSTOMER_DEMOGRAPHICS", customer_demographics);

    QuarkTable date_dim = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("d_date_sk", "int"))
        .add(new QuarkColumn("d_date_id", "string"))
        .add(new QuarkColumn("d_date", "date"))
        .add(new QuarkColumn("d_month_seq", "int"))
        .add(new QuarkColumn("d_week_seq", "int"))
        .add(new QuarkColumn("d_quarter_seq", "int"))
        .add(new QuarkColumn("d_year", "int"))
        .add(new QuarkColumn("d_dow", "int"))
        .add(new QuarkColumn("d_moy", "int"))
        .add(new QuarkColumn("d_dom", "int"))
        .add(new QuarkColumn("d_qoy", "int"))
        .add(new QuarkColumn("d_fy_year", "int"))
        .add(new QuarkColumn("d_fy_quarter_seq", "int"))
        .add(new QuarkColumn("d_fy_week_seq", "int"))
        .add(new QuarkColumn("d_day_name", "string"))
        .add(new QuarkColumn("d_quarter_name", "string"))
        .add(new QuarkColumn("d_holiday", "string"))
        .add(new QuarkColumn("d_weekend", "string"))
        .add(new QuarkColumn("d_following_holiday", "string"))
        .add(new QuarkColumn("d_first_dom", "int"))
        .add(new QuarkColumn("d_last_dom", "int"))
        .add(new QuarkColumn("d_same_day_ly", "int"))
        .add(new QuarkColumn("d_same_day_lq", "int"))
        .add(new QuarkColumn("d_current_day", "string"))
        .add(new QuarkColumn("d_current_week", "string"))
        .add(new QuarkColumn("d_current_month", "string"))
        .add(new QuarkColumn("d_current_quarter", "string"))
        .add(new QuarkColumn("d_current_year", "string")).build()
    );

    builder.put("DATE_DIM", date_dim);

    QuarkTable time_dim = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("t_time_sk", "int"))
        .add(new QuarkColumn("t_time_id", "string"))
        .add(new QuarkColumn("t_time", "int"))
        .add(new QuarkColumn("t_hour", "int"))
        .add(new QuarkColumn("t_minute", "int"))
        .add(new QuarkColumn("t_second", "int"))
        .add(new QuarkColumn("t_am_pm", "sting"))
        .add(new QuarkColumn("t_shift", "string"))
        .add(new QuarkColumn("t_sub_shift", "string"))
        .add(new QuarkColumn("t_meal_time", "string")).build()
    );

    builder.put("TIME_DIM", time_dim);

    QuarkTable item = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_sk", "int"))
        .add(new QuarkColumn("i_item_id", "string"))
        .add(new QuarkColumn("i_rec_start_date", "date"))
        .add(new QuarkColumn("i_rec_end_date", "date"))
        .add(new QuarkColumn("i_item_desc", "string"))
        .add(new QuarkColumn("i_current_price", "double"))
        .add(new QuarkColumn("i_wholesale_cost", "double"))
        .add(new QuarkColumn("i_brand_id", "int"))
        .add(new QuarkColumn("i_brand", "string"))
        .add(new QuarkColumn("i_class_id", "int"))
        .add(new QuarkColumn("i_class", "string"))
        .add(new QuarkColumn("i_category_id", "int"))
        .add(new QuarkColumn("i_category", "string"))
        .add(new QuarkColumn("i_manufact_id", "int"))
        .add(new QuarkColumn("i_manufact", "string"))
        .add(new QuarkColumn("i_size", "string"))
        .add(new QuarkColumn("i_formulation", "string"))
        .add(new QuarkColumn("i_color", "string"))
        .add(new QuarkColumn("i_units", "string"))
        .add(new QuarkColumn("i_container", "string"))
        .add(new QuarkColumn("i_manager_id", "int"))
        .add(new QuarkColumn("i_product_name", "string")).build()
    );

    builder.put("ITEM", item);

    QuarkTable customer = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("c_customer_sk", "int"))
        .add(new QuarkColumn("c_customer_id", "string"))
        .add(new QuarkColumn("c_current_cdemo_sk", "int"))
        .add(new QuarkColumn("c_current_hdemo_sk", "int"))
        .add(new QuarkColumn("c_current_addr_sk", "int"))
        .add(new QuarkColumn("c_first_shipto_date_sk", "int"))
        .add(new QuarkColumn("c_first_sales_date_sk", "int"))
        .add(new QuarkColumn("c_salutation", "string"))
        .add(new QuarkColumn("c_first_name", "string"))
        .add(new QuarkColumn("c_last_name", "string"))
        .add(new QuarkColumn("c_preferred_cust_flag", "string"))
        .add(new QuarkColumn("c_birth_day", "int"))
        .add(new QuarkColumn("c_birth_month", "int"))
        .add(new QuarkColumn("c_birth_year", "int"))
        .add(new QuarkColumn("c_birth_country", "string"))
        .add(new QuarkColumn("c_login", "string"))
        .add(new QuarkColumn("c_email_address", "string"))
        .add(new QuarkColumn("c_last_review_date", "string")).build()
    );

    builder.put("CUSTOMER", customer);

    QuarkTable web_returns = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("wr_returned_date_sk", "int"))
        .add(new QuarkColumn("wr_returned_time_sk", "int"))
        .add(new QuarkColumn("wr_item_sk", "int"))
        .add(new QuarkColumn("wr_refunded_customer_sk", "int"))
        .add(new QuarkColumn("wr_refunded_cdemo_sk", "int"))
        .add(new QuarkColumn("wr_refunded_hdemo_sk", "int"))
        .add(new QuarkColumn("wr_refunded_addr_sk", "int"))
        .add(new QuarkColumn("wr_returning_customer_sk", "int"))
        .add(new QuarkColumn("wr_returning_cdemo_sk", "int"))
        .add(new QuarkColumn("wr_returning_hdemo_sk", "int"))
        .add(new QuarkColumn("wr_returning_addr_sk", "int"))
        .add(new QuarkColumn("wr_web_page_sk", "int"))
        .add(new QuarkColumn("wr_reason_sk", "int"))
        .add(new QuarkColumn("wr_order_number", "int"))
        .add(new QuarkColumn("wr_return_quantity", "int"))
        .add(new QuarkColumn("wr_return_amt", "double"))
        .add(new QuarkColumn("wr_return_tax", "double"))
        .add(new QuarkColumn("wr_return_amt_inc_tax", "double"))
        .add(new QuarkColumn("wr_fee", "double"))
        .add(new QuarkColumn("wr_return_ship_cost", "double"))
        .add(new QuarkColumn("wr_refunded_cash", "double"))
        .add(new QuarkColumn("wr_reversed_charge", "double"))
        .add(new QuarkColumn("wr_account_credit", "double"))
        .add(new QuarkColumn("wr_net_loss", "double")).build()
    );

    builder.put("WEB_RETURNS", web_returns);

    QuarkTable web_returns_cube = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_id", "string"))
        .add(new QuarkColumn("d_year", "int"))
        .add(new QuarkColumn("d_qoy", "int"))
        .add(new QuarkColumn("d_moy", "int"))
        .add(new QuarkColumn("d_date", "int"))
        .add(new QuarkColumn("cd_gender", "string"))
        .add(new QuarkColumn("cd_marital_status", "string"))
        .add(new QuarkColumn("cd_education_status", "string"))
        .add(new QuarkColumn("grouping_id", "string"))
        .add(new QuarkColumn("total_net_loss", "double")).build()
    );

    builder.put("WEB_RETURNS_CUBE", web_returns_cube);

    QuarkTable store_sales = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("ss_sold_date_sk", "int"))
        .add(new QuarkColumn("ss_sold_time_sk", "int"))
        .add(new QuarkColumn("ss_item_sk", "int"))
        .add(new QuarkColumn("ss_customer_sk", "int"))
        .add(new QuarkColumn("ss_cdemo_sk", "int"))
        .add(new QuarkColumn("ss_hdemo_sk", "int"))
        .add(new QuarkColumn("ss_addr_sk", "int"))
        .add(new QuarkColumn("ss_store_sk", "int"))
        .add(new QuarkColumn("ss_promo_sk", "int"))
        .add(new QuarkColumn("ss_ticket_number", "int"))
        .add(new QuarkColumn("ss_quantity", "int"))
        .add(new QuarkColumn("ss_wholesale_cost", "double"))
        .add(new QuarkColumn("ss_list_price", "double"))
        .add(new QuarkColumn("ss_sales_price", "double"))
        .add(new QuarkColumn("ss_ext_discount_amt", "double"))
        .add(new QuarkColumn("ss_ext_sales_price", "double"))
        .add(new QuarkColumn("ss_ext_wholesale_cost", "double"))
        .add(new QuarkColumn("ss_ext_list_price", "double"))
        .add(new QuarkColumn("ss_ext_tax", "double"))
        .add(new QuarkColumn("ss_coupon_amt", "double"))
        .add(new QuarkColumn("ss_net_paid", "double"))
        .add(new QuarkColumn("ss_net_paid_inc_tax", "double"))
        .add(new QuarkColumn("ss_net_profit", "double")).build()
    );

    builder.put("STORE_SALES", store_sales);

    QuarkTable store_sales_cube = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_id", "string"))
        .add(new QuarkColumn("c_customer_id", "string"))
        .add(new QuarkColumn("d_year", "int"))
        .add(new QuarkColumn("d_qoy", "int"))
        .add(new QuarkColumn("d_moy", "int"))
        .add(new QuarkColumn("d_date", "int"))
        .add(new QuarkColumn("cd_gender", "string"))
        .add(new QuarkColumn("cd_marital_status", "string"))
        .add(new QuarkColumn("cd_education_status", "string"))
        .add(new QuarkColumn("grouping_id", "string"))
        .add(new QuarkColumn("sum_sales_price", "double"))
        .add(new QuarkColumn("sum_extended_sales_price", "double")).build()
        );

    builder.put("STORE_SALES_CUBE", store_sales_cube);
    return builder.build();
  }
}
