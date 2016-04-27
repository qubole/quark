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

import java.sql.Types;
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
        .add(new QuarkColumn("cd_demo_sk", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("cd_marital_status", Types.VARCHAR))
        .add(new QuarkColumn("cd_education_status", Types.VARCHAR))
        .add(new QuarkColumn("cd_purchase_estimate", Types.INTEGER))
        .add(new QuarkColumn("cd_credit_rating", Types.VARCHAR))
        .add(new QuarkColumn("cd_dep_count", Types.INTEGER))
        .add(new QuarkColumn("cd_dep_employed_count", Types.INTEGER))
        .add(new QuarkColumn("cd_dep_college_count", Types.INTEGER)).build()
    );

    builder.put("CUSTOMER_DEMOGRAPHICS", customer_demographics);

    QuarkTable date_dim = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("d_date_sk", Types.INTEGER))
        .add(new QuarkColumn("d_date_id", Types.VARCHAR))
        .add(new QuarkColumn("d_date", Types.DATE))
        .add(new QuarkColumn("d_month_seq", Types.INTEGER))
        .add(new QuarkColumn("d_week_seq", Types.INTEGER))
        .add(new QuarkColumn("d_quarter_seq", Types.INTEGER))
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_dow", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("d_dom", Types.INTEGER))
        .add(new QuarkColumn("d_qoy", Types.INTEGER))
        .add(new QuarkColumn("d_fy_year", Types.INTEGER))
        .add(new QuarkColumn("d_fy_quarter_seq", Types.INTEGER))
        .add(new QuarkColumn("d_fy_week_seq", Types.INTEGER))
        .add(new QuarkColumn("d_day_name", Types.VARCHAR))
        .add(new QuarkColumn("d_quarter_name", Types.VARCHAR))
        .add(new QuarkColumn("d_holiday", Types.VARCHAR))
        .add(new QuarkColumn("d_weekend", Types.VARCHAR))
        .add(new QuarkColumn("d_following_holiday", Types.VARCHAR))
        .add(new QuarkColumn("d_first_dom", Types.INTEGER))
        .add(new QuarkColumn("d_last_dom", Types.INTEGER))
        .add(new QuarkColumn("d_same_day_ly", Types.INTEGER))
        .add(new QuarkColumn("d_same_day_lq", Types.INTEGER))
        .add(new QuarkColumn("d_current_day", Types.VARCHAR))
        .add(new QuarkColumn("d_current_week", Types.VARCHAR))
        .add(new QuarkColumn("d_current_month", Types.VARCHAR))
        .add(new QuarkColumn("d_current_quarter", Types.VARCHAR))
        .add(new QuarkColumn("d_current_year", Types.VARCHAR)).build()
    );

    builder.put("DATE_DIM", date_dim);

    QuarkTable time_dim = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("t_time_sk", Types.INTEGER))
        .add(new QuarkColumn("t_time_id", Types.VARCHAR))
        .add(new QuarkColumn("t_time", Types.INTEGER))
        .add(new QuarkColumn("t_hour", Types.INTEGER))
        .add(new QuarkColumn("t_minute", Types.INTEGER))
        .add(new QuarkColumn("t_second", Types.INTEGER))
        .add(new QuarkColumn("t_am_pm", Types.VARCHAR))
        .add(new QuarkColumn("t_shift", Types.VARCHAR))
        .add(new QuarkColumn("t_sub_shift", Types.VARCHAR))
        .add(new QuarkColumn("t_meal_time", Types.VARCHAR)).build()
    );

    builder.put("TIME_DIM", time_dim);

    QuarkTable item = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_sk", Types.INTEGER))
        .add(new QuarkColumn("i_item_id", Types.VARCHAR))
        .add(new QuarkColumn("i_rec_start_date", Types.DATE))
        .add(new QuarkColumn("i_rec_end_date", Types.DATE))
        .add(new QuarkColumn("i_item_desc", Types.VARCHAR))
        .add(new QuarkColumn("i_current_price", Types.DOUBLE))
        .add(new QuarkColumn("i_wholesale_cost", Types.DOUBLE))
        .add(new QuarkColumn("i_brand_id", Types.INTEGER))
        .add(new QuarkColumn("i_brand", Types.VARCHAR))
        .add(new QuarkColumn("i_class_id", Types.INTEGER))
        .add(new QuarkColumn("i_class", Types.VARCHAR))
        .add(new QuarkColumn("i_category_id", Types.INTEGER))
        .add(new QuarkColumn("i_category", Types.VARCHAR))
        .add(new QuarkColumn("i_manufact_id", Types.INTEGER))
        .add(new QuarkColumn("i_manufact", Types.VARCHAR))
        .add(new QuarkColumn("i_size", Types.VARCHAR))
        .add(new QuarkColumn("i_formulation", Types.VARCHAR))
        .add(new QuarkColumn("i_color", Types.VARCHAR))
        .add(new QuarkColumn("i_units", Types.VARCHAR))
        .add(new QuarkColumn("i_container", Types.VARCHAR))
        .add(new QuarkColumn("i_manager_id", Types.INTEGER))
        .add(new QuarkColumn("i_product_name", Types.VARCHAR)).build()
    );

    builder.put("ITEM", item);

    QuarkTable customer = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("c_customer_sk", Types.INTEGER))
        .add(new QuarkColumn("c_customer_id", Types.VARCHAR))
        .add(new QuarkColumn("c_current_cdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("c_current_hdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("c_current_addr_sk", Types.INTEGER))
        .add(new QuarkColumn("c_first_shipto_date_sk", Types.INTEGER))
        .add(new QuarkColumn("c_first_sales_date_sk", Types.INTEGER))
        .add(new QuarkColumn("c_salutation", Types.VARCHAR))
        .add(new QuarkColumn("c_first_name", Types.VARCHAR))
        .add(new QuarkColumn("c_last_name", Types.VARCHAR))
        .add(new QuarkColumn("c_preferred_cust_flag", Types.VARCHAR))
        .add(new QuarkColumn("c_birth_day", Types.INTEGER))
        .add(new QuarkColumn("c_birth_month", Types.INTEGER))
        .add(new QuarkColumn("c_birth_year", Types.INTEGER))
        .add(new QuarkColumn("c_birth_country", Types.VARCHAR))
        .add(new QuarkColumn("c_login", Types.VARCHAR))
        .add(new QuarkColumn("c_email_address", Types.VARCHAR))
        .add(new QuarkColumn("c_last_review_date", Types.VARCHAR)).build()
    );

    builder.put("CUSTOMER", customer);

    QuarkTable web_returns = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("wr_returned_date_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_returned_time_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_item_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_refunded_customer_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_refunded_cdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_refunded_hdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_refunded_addr_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_returning_customer_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_returning_cdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_returning_hdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_returning_addr_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_web_page_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_reason_sk", Types.INTEGER))
        .add(new QuarkColumn("wr_order_number", Types.INTEGER))
        .add(new QuarkColumn("wr_return_quantity", Types.INTEGER))
        .add(new QuarkColumn("wr_return_amt", Types.DOUBLE))
        .add(new QuarkColumn("wr_return_tax", Types.DOUBLE))
        .add(new QuarkColumn("wr_return_amt_inc_tax", Types.DOUBLE))
        .add(new QuarkColumn("wr_fee", Types.DOUBLE))
        .add(new QuarkColumn("wr_return_ship_cost", Types.DOUBLE))
        .add(new QuarkColumn("wr_refunded_cash", Types.DOUBLE))
        .add(new QuarkColumn("wr_reversed_charge", Types.DOUBLE))
        .add(new QuarkColumn("wr_account_credit", Types.DOUBLE))
        .add(new QuarkColumn("wr_net_loss", Types.DOUBLE)).build()
    );

    builder.put("WEB_RETURNS", web_returns);

    QuarkTable web_returns_cube = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_id", Types.VARCHAR))
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_qoy", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("d_date", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("cd_marital_status", Types.VARCHAR))
        .add(new QuarkColumn("cd_education_status", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("total_net_loss", Types.DOUBLE)).build()
    );

    builder.put("WEB_RETURNS_CUBE", web_returns_cube);

    QuarkTable store_sales = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("ss_sold_date_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_sold_time_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_item_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_customer_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_cdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_hdemo_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_addr_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_store_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_promo_sk", Types.INTEGER))
        .add(new QuarkColumn("ss_ticket_number", Types.INTEGER))
        .add(new QuarkColumn("ss_quantity", Types.INTEGER))
        .add(new QuarkColumn("ss_wholesale_cost", Types.DOUBLE))
        .add(new QuarkColumn("ss_list_price", Types.DOUBLE))
        .add(new QuarkColumn("ss_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("ss_ext_discount_amt", Types.DOUBLE))
        .add(new QuarkColumn("ss_ext_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("ss_ext_wholesale_cost", Types.DOUBLE))
        .add(new QuarkColumn("ss_ext_list_price", Types.DOUBLE))
        .add(new QuarkColumn("ss_ext_tax", Types.DOUBLE))
        .add(new QuarkColumn("ss_coupon_amt", Types.DOUBLE))
        .add(new QuarkColumn("ss_net_paid", Types.DOUBLE))
        .add(new QuarkColumn("ss_net_paid_inc_tax", Types.DOUBLE))
        .add(new QuarkColumn("ss_net_profit", Types.DOUBLE)).build()
    );

    builder.put("STORE_SALES", store_sales);

    QuarkTable web_site = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("web_site_sk", Types.INTEGER))
        .add(new QuarkColumn("web_site_id", Types.CHAR))
        .add(new QuarkColumn("web_rec_start_date", Types.DATE))
        .add(new QuarkColumn("web_rec_end_data", Types.DATE))
        .add(new QuarkColumn("web_name", Types.VARCHAR))
        .add(new QuarkColumn("web_open_date_sk", Types.INTEGER))
            .add(new QuarkColumn("web_close_date_sk", Types.INTEGER))
            .add(new QuarkColumn("web_class", Types.VARCHAR))
            .add(new QuarkColumn("web_manager", Types.VARCHAR))
            .add(new QuarkColumn("web_mkt_id", Types.INTEGER))
            .add(new QuarkColumn("web_mkt_class", Types.VARCHAR))
            .add(new QuarkColumn("web_mkt_desc", Types.VARCHAR))
            .add(new QuarkColumn("web_market_manager", Types.VARCHAR))
            .add(new QuarkColumn("web_company_id", Types.INTEGER))
            .add(new QuarkColumn("web_company_name", Types.CHAR))
            .add(new QuarkColumn("web_street_number", Types.CHAR))
            .add(new QuarkColumn("web_street_name", Types.VARCHAR))
            .add(new QuarkColumn("web_street_type", Types.CHAR))
            .add(new QuarkColumn("web_suite_number", Types.CHAR))
            .add(new QuarkColumn("web_city", Types.VARCHAR))
            .add(new QuarkColumn("web_county", Types.VARCHAR))
            .add(new QuarkColumn("web_state", Types.CHAR))
            .add(new QuarkColumn("web_zip", Types.CHAR))
            .add(new QuarkColumn("web_country", Types.VARCHAR))
            .add(new QuarkColumn("web_gmt_offset", Types.DECIMAL))
            .add(new QuarkColumn("web_tax_percentage", Types.DECIMAL)).build()
    );

    builder.put("WEB_SITE", web_site);

    QuarkTable store_sales_cube = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_id", Types.VARCHAR))
        .add(new QuarkColumn("c_customer_id", Types.VARCHAR))
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_qoy", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("d_date", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("cd_marital_status", Types.VARCHAR))
        .add(new QuarkColumn("cd_education_status", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("sum_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("sum_extended_sales_price", Types.DOUBLE)).build()
        );

    builder.put("STORE_SALES_CUBE", store_sales_cube);

    QuarkTable store_sales_cube_partial = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("i_item_id", Types.VARCHAR))
        .add(new QuarkColumn("c_customer_id", Types.VARCHAR))
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("d_dom", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("cd_marital_status", Types.VARCHAR))
        .add(new QuarkColumn("cd_education_status", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("sum_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("sum_extended_sales_price", Types.DOUBLE)).build()
    );

    builder.put("STORE_SALES_CUBE_PARTIAL", store_sales_cube_partial);

    QuarkTable store_sales_cube_daily = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("d_dom", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("sum_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("sum_extended_sales_price", Types.DOUBLE)).build()
    );

    builder.put("STORE_SALES_CUBE_DAILY", store_sales_cube_daily);

    QuarkTable store_sales_cube_weekly = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("d_week_seq", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("sum_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("sum_extended_sales_price", Types.DOUBLE)).build()
    );

    builder.put("STORE_SALES_CUBE_WEEKLY", store_sales_cube_weekly);

    QuarkTable store_sales_cube_monthly = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("d_year", Types.INTEGER))
        .add(new QuarkColumn("d_moy", Types.INTEGER))
        .add(new QuarkColumn("cd_gender", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("sum_sales_price", Types.DOUBLE))
        .add(new QuarkColumn("sum_extended_sales_price", Types.DOUBLE)).build()
    );

    builder.put("STORE_SALES_CUBE_MONTHLY", store_sales_cube_monthly);

    QuarkTable web_site_partition = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
            .add(new QuarkColumn("web_site_sk", Types.INTEGER))
            .add(new QuarkColumn("web_rec_start_date", Types.DATE))
            .add(new QuarkColumn("web_county", Types.VARCHAR))
            .add(new QuarkColumn("web_tax_percentage", Types.DECIMAL)).build()
    );

    builder.put("WEB_SITE_PARTITION", web_site_partition);
    return builder.build();
  }
}
