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
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by rajatv on 3/19/15.
 */
public class Foodmart extends TestSchema {
  Foodmart(String name) {
    super(name);
  }

  @Override
  public Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    QuarkTable account = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("account_id", Types.INTEGER));
      add(new QuarkColumn("account_parent", Types.INTEGER));
      add(new QuarkColumn("account_description", Types.VARCHAR));
      add(new QuarkColumn("account_type", Types.VARCHAR));
      add(new QuarkColumn("account_rollup", Types.VARCHAR));
      add(new QuarkColumn("Custom_Members", Types.VARCHAR));
    }});
    builder.put("ACCOUNT", account);

    QuarkTable category = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("category_id", Types.VARCHAR));
      add(new QuarkColumn("category_parent", Types.VARCHAR));
      add(new QuarkColumn("category_description", Types.VARCHAR));
      add(new QuarkColumn("category_rollup", Types.VARCHAR));
    }});
    builder.put("CATEGORY", category);

    QuarkTable currency = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("currency_id", Types.INTEGER));
      add(new QuarkColumn("date", Types.DATE));
      add(new QuarkColumn("currency", Types.VARCHAR));
      add(new QuarkColumn("currency_ratio", Types.DOUBLE));
    }});
    builder.put("CURRENCY", currency);

    QuarkTable customer = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("customer_id", Types.INTEGER));
      add(new QuarkColumn("account_num", Types.INTEGER));
      add(new QuarkColumn("lname", Types.VARCHAR));
      add(new QuarkColumn("fname", Types.VARCHAR));
      add(new QuarkColumn("mi", Types.VARCHAR));
      add(new QuarkColumn("address1", Types.VARCHAR));
      add(new QuarkColumn("address2", Types.VARCHAR));
      add(new QuarkColumn("address3", Types.VARCHAR));
      add(new QuarkColumn("address4", Types.VARCHAR));
      add(new QuarkColumn("city", Types.VARCHAR));
      add(new QuarkColumn("state_province", Types.VARCHAR));
      add(new QuarkColumn("postal_code", Types.VARCHAR));
      add(new QuarkColumn("country", Types.VARCHAR));
      add(new QuarkColumn("customer_region", Types.VARCHAR));
      add(new QuarkColumn("phone1", Types.VARCHAR));
      add(new QuarkColumn("phone2", Types.VARCHAR));
      add(new QuarkColumn("birthdate", Types.DATE));
      add(new QuarkColumn("marital_status", Types.VARCHAR));
      add(new QuarkColumn("yearly_income", Types.VARCHAR));
      add(new QuarkColumn("gender", Types.VARCHAR));
      add(new QuarkColumn("total_children", Types.INTEGER));
      add(new QuarkColumn("num_children_at_home", Types.INTEGER));
      add(new QuarkColumn("education", Types.VARCHAR));
      add(new QuarkColumn("date_accnt_opened", Types.DATE));
      add(new QuarkColumn("member_card", Types.VARCHAR));
      add(new QuarkColumn("occupation", Types.VARCHAR));
      add(new QuarkColumn("houseowner", Types.VARCHAR));
      add(new QuarkColumn("num_cars_owned", Types.INTEGER));
      add(new QuarkColumn("full_name", Types.VARCHAR));
    }});
    builder.put("CUSTOMER", customer);

    QuarkTable days = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("day", Types.INTEGER));
      add(new QuarkColumn("week_day", Types.VARCHAR));
    }});
    builder.put("DAYS", days);

    QuarkTable department = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("department_id", Types.INTEGER));
      add(new QuarkColumn("department_description", Types.VARCHAR));
    }});
    builder.put("DEPARTMENT", department);

    QuarkTable employee = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("employee_id", Types.INTEGER));
      add(new QuarkColumn("full_name", Types.VARCHAR));
      add(new QuarkColumn("first_name", Types.VARCHAR));
      add(new QuarkColumn("last_name", Types.VARCHAR));
      add(new QuarkColumn("position_id", Types.INTEGER));
      add(new QuarkColumn("position_title", Types.VARCHAR));
      add(new QuarkColumn("store_id", Types.INTEGER));
      add(new QuarkColumn("department_id", Types.INTEGER));
      add(new QuarkColumn("birth_date", Types.DATE));
      add(new QuarkColumn("hire_date", Types.TIMESTAMP));
      add(new QuarkColumn("end_date", Types.TIMESTAMP));
      add(new QuarkColumn("supervisor_id", Types.INTEGER));
      add(new QuarkColumn("education_level", Types.VARCHAR));
      add(new QuarkColumn("marital_status", Types.VARCHAR));
      add(new QuarkColumn("gender", Types.VARCHAR));
      add(new QuarkColumn("management_role", Types.VARCHAR));
    }});
    builder.put("EMPLOYEE", employee);

    QuarkTable employee_closure = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("employee_id", Types.INTEGER));
      add(new QuarkColumn("supervisor_id", Types.INTEGER));
      add(new QuarkColumn("distance", Types.INTEGER));
    }});
    builder.put("EMPLOYEE_CLOSURE", employee_closure);

    QuarkTable expense_fact = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("store_id", Types.INTEGER));
      add(new QuarkColumn("account_id", Types.INTEGER));
      add(new QuarkColumn("exp_date", Types.TIMESTAMP));
      add(new QuarkColumn("category_id", Types.VARCHAR));
      add(new QuarkColumn("currency_id", Types.INTEGER));
      add(new QuarkColumn("amount", Types.DOUBLE));
    }});
    builder.put("EXPENSE_FACT", expense_fact);

    QuarkTable position = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("position_id", Types.INTEGER));
      add(new QuarkColumn("position_title", Types.VARCHAR));
      add(new QuarkColumn("pay_type", Types.VARCHAR));
      add(new QuarkColumn("min_scale", Types.DOUBLE));
      add(new QuarkColumn("max_scale", Types.DOUBLE));
      add(new QuarkColumn("management_role", Types.VARCHAR));
    }});
    builder.put("POSITION", position);

    QuarkTable product = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_class_id", Types.INTEGER));
      add(new QuarkColumn("product_id", Types.INTEGER));
      add(new QuarkColumn("brand_name", Types.VARCHAR));
      add(new QuarkColumn("sku", Types.INTEGER));
      add(new QuarkColumn("srp", Types.DOUBLE));
      add(new QuarkColumn("gross_weight", Types.DOUBLE));
      add(new QuarkColumn("net_weight", Types.DOUBLE));
      add(new QuarkColumn("recyclable_package", Types.INTEGER));
      add(new QuarkColumn("low_fat", Types.INTEGER));
      add(new QuarkColumn("units_per_case", Types.INTEGER));
      add(new QuarkColumn("cases_per_pallet", Types.INTEGER));
      add(new QuarkColumn("shelf_width", Types.DOUBLE));
      add(new QuarkColumn("shelf_height", Types.DOUBLE));
      add(new QuarkColumn("shelf_depth", Types.DOUBLE));
    }});
    builder.put("PRODUCT", product);

    QuarkTable product_class = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_class_id", Types.INTEGER));
      add(new QuarkColumn("product_sub_category", Types.VARCHAR));
      add(new QuarkColumn("product_category", Types.VARCHAR));
      add(new QuarkColumn("product_department", Types.VARCHAR));
      add(new QuarkColumn("product_family", Types.VARCHAR));
    }});
    builder.put("PRODUCT_CLASS", product_class);

    QuarkTable promotion = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("promotion_id", Types.INTEGER));
      add(new QuarkColumn("promotion_district_id", Types.INTEGER));
      add(new QuarkColumn("promotion_name", Types.VARCHAR));
      add(new QuarkColumn("media_type", Types.VARCHAR));
      add(new QuarkColumn("cost", Types.DOUBLE));
      add(new QuarkColumn("start_date", Types.DATE));
      add(new QuarkColumn("end_date", Types.DATE));
    }});
    builder.put("PROMOTION", promotion);

    QuarkTable region = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("region_id", Types.INTEGER));
      add(new QuarkColumn("sales_city", Types.VARCHAR));
      add(new QuarkColumn("sales_state_province", Types.VARCHAR));
      add(new QuarkColumn("sales_district", Types.VARCHAR));
      add(new QuarkColumn("sales_region", Types.VARCHAR));
      add(new QuarkColumn("sales_country", Types.VARCHAR));
      add(new QuarkColumn("sales_district_id", Types.INTEGER));
    }});
    builder.put("REGION", region);

    QuarkTable reserve_employee = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("employee_id", Types.INTEGER));
      add(new QuarkColumn("full_name", Types.VARCHAR));
      add(new QuarkColumn("first_name", Types.VARCHAR));
      add(new QuarkColumn("last_name", Types.VARCHAR));
      add(new QuarkColumn("position_id", Types.INTEGER));
      add(new QuarkColumn("position_title", Types.VARCHAR));
      add(new QuarkColumn("store_id", Types.INTEGER));
      add(new QuarkColumn("department_id", Types.INTEGER));
      add(new QuarkColumn("birth_date", Types.DATE));
      add(new QuarkColumn("hire_date", Types.TIMESTAMP));
      add(new QuarkColumn("end_date", Types.TIMESTAMP));
      add(new QuarkColumn("supervisor_id", Types.INTEGER));
      add(new QuarkColumn("education_level", Types.VARCHAR));
      add(new QuarkColumn("marital_status", Types.VARCHAR));
      add(new QuarkColumn("gender", Types.VARCHAR));
      add(new QuarkColumn("management_role", Types.VARCHAR));
    }});
    builder.put("RESERVE_EMPLOYEE", reserve_employee);

    QuarkTable salary = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("pay_date", Types.TIMESTAMP));
      add(new QuarkColumn("employee_id", Types.INTEGER));
      add(new QuarkColumn("department_id", Types.INTEGER));
      add(new QuarkColumn("currency_id", Types.INTEGER));
      add(new QuarkColumn("salary_paid", Types.DOUBLE));
      add(new QuarkColumn("overtime_paid", Types.DOUBLE));
      add(new QuarkColumn("vacation_accrued", Types.DOUBLE));
      add(new QuarkColumn("vacation_used", Types.DOUBLE));
    }});
    builder.put("SALARY", salary);

    QuarkTable store = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("store_id", Types.INTEGER));
      add(new QuarkColumn("store_type", Types.VARCHAR));
      add(new QuarkColumn("region_id", Types.INTEGER));
      add(new QuarkColumn("store_name", Types.VARCHAR));
      add(new QuarkColumn("store_number", Types.INTEGER));
      add(new QuarkColumn("store_street_address", Types.VARCHAR));
      add(new QuarkColumn("store_city", Types.VARCHAR));
      add(new QuarkColumn("store_state", Types.VARCHAR));
      add(new QuarkColumn("store_postal_code", Types.VARCHAR));
      add(new QuarkColumn("store_country", Types.VARCHAR));
      add(new QuarkColumn("store_manager", Types.VARCHAR));
      add(new QuarkColumn("store_phone", Types.VARCHAR));
      add(new QuarkColumn("store_fax", Types.VARCHAR));
      add(new QuarkColumn("first_opened_date", Types.TIMESTAMP));
      add(new QuarkColumn("last_remodel_date", Types.TIMESTAMP));
      add(new QuarkColumn("store_sqft", Types.INTEGER));
      add(new QuarkColumn("grocery_sqft", Types.INTEGER));
      add(new QuarkColumn("frozen_sqft", Types.INTEGER));
      add(new QuarkColumn("meat_sqft", Types.INTEGER));
      add(new QuarkColumn("coffee_bar", Types.INTEGER));
      add(new QuarkColumn("video_store", Types.INTEGER));
      add(new QuarkColumn("salad_bar", Types.INTEGER));
      add(new QuarkColumn("prepared_food", Types.INTEGER));
      add(new QuarkColumn("florist", Types.INTEGER));
    }});
    builder.put("STORE", store);

    QuarkTable sales_fact_1997 = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_id", Types.INTEGER));
      add(new QuarkColumn("time_id", Types.INTEGER));
      add(new QuarkColumn("customer_id", Types.INTEGER));
      add(new QuarkColumn("promotion_id", Types.INTEGER));
      add(new QuarkColumn("store_id", Types.INTEGER));
      add(new QuarkColumn("store_sales", Types.DOUBLE));
      add(new QuarkColumn("store_cost", Types.DOUBLE));
      add(new QuarkColumn("unit_sales", Types.DOUBLE));
    }});
    builder.put("SALES_FACT_1997", sales_fact_1997);

    QuarkTable time_by_day = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("time_id", Types.INTEGER));
      add(new QuarkColumn("the_date", Types.TIMESTAMP));
      add(new QuarkColumn("the_day", Types.VARCHAR));
      add(new QuarkColumn("the_month", Types.VARCHAR));
      add(new QuarkColumn("the_year", Types.INTEGER));
      add(new QuarkColumn("day_of_month", Types.INTEGER));
      add(new QuarkColumn("week_of_year", Types.INTEGER));
      add(new QuarkColumn("month_of_year", Types.INTEGER));
      add(new QuarkColumn("quarter", Types.VARCHAR));
      add(new QuarkColumn("fiscal_period", Types.VARCHAR));
    }});
    builder.put("TIME_BY_DAY", time_by_day);

    QuarkTable warehouse = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("warehouse_id", Types.INTEGER));
      add(new QuarkColumn("warehouse_class_id", Types.INTEGER));
      add(new QuarkColumn("stores_id", Types.INTEGER));
      add(new QuarkColumn("warehouse_name", Types.VARCHAR));
      add(new QuarkColumn("wa_address1", Types.VARCHAR));
      add(new QuarkColumn("wa_address2", Types.VARCHAR));
      add(new QuarkColumn("wa_address3", Types.VARCHAR));
      add(new QuarkColumn("wa_address4", Types.VARCHAR));
      add(new QuarkColumn("warehouse_city", Types.VARCHAR));
      add(new QuarkColumn("warehouse_state", Types.VARCHAR));
      add(new QuarkColumn("warehouse_postal_code", Types.VARCHAR));
      add(new QuarkColumn("warehouse_country", Types.VARCHAR));
      add(new QuarkColumn("warehouse_owner_name", Types.VARCHAR));
      add(new QuarkColumn("warehouse_phone", Types.VARCHAR));
      add(new QuarkColumn("warehouse_fax", Types.VARCHAR));
    }});
    builder.put("WAREHOUSE", warehouse);

    QuarkTable warehouse_class = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("warehouse_class_id", Types.INTEGER));
      add(new QuarkColumn("description", Types.VARCHAR));
    }});
    builder.put("WAREHOUSE_CLASS", warehouse_class);

    QuarkTable inventory_fact = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_id", Types.INTEGER));
      add(new QuarkColumn("time_id", Types.INTEGER));
      add(new QuarkColumn("warehouse_id", Types.INTEGER));
      add(new QuarkColumn("store_id", Types.INTEGER));
      add(new QuarkColumn("units_ordered", Types.INTEGER));
      add(new QuarkColumn("units_shipped", Types.INTEGER));
      add(new QuarkColumn("warehouse_sales", Types.INTEGER));
      add(new QuarkColumn("supply_time", Types.INTEGER));
      add(new QuarkColumn("store_invoice", Types.INTEGER));
    }});
    builder.put("INVENTORY_FACT", inventory_fact);

    QuarkTable count_fact_tile = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("the_year", Types.INTEGER))
        .add(new QuarkColumn("quarter", Types.VARCHAR))
        .add(new QuarkColumn("grouping_id", Types.VARCHAR))
        .add(new QuarkColumn("total_store_sales", Types.DOUBLE))
        .add(new QuarkColumn("total_unit_sales", Types.DOUBLE)).build()
    );
    builder.put("COUNT_FACT_TILE", count_fact_tile);

    return builder.build();
  }
}
