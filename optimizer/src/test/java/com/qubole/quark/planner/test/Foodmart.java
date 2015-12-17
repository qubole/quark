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
      add(new QuarkColumn("account_id", "int"));
      add(new QuarkColumn("account_parent", "int"));
      add(new QuarkColumn("account_description", "string"));
      add(new QuarkColumn("account_type", "string"));
      add(new QuarkColumn("account_rollup", "string"));
      add(new QuarkColumn("Custom_Members", "string"));
    }});
    builder.put("ACCOUNT", account);

    QuarkTable category = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("category_id", "string"));
      add(new QuarkColumn("category_parent", "string"));
      add(new QuarkColumn("category_description", "string"));
      add(new QuarkColumn("category_rollup", "string"));
    }});
    builder.put("CATEGORY", category);

    QuarkTable currency = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("currency_id", "int"));
      add(new QuarkColumn("date", "date"));
      add(new QuarkColumn("currency", "string"));
      add(new QuarkColumn("currency_ratio", "double"));
    }});
    builder.put("CURRENCY", currency);

    QuarkTable customer = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("customer_id", "int"));
      add(new QuarkColumn("account_num", "int"));
      add(new QuarkColumn("lname", "string"));
      add(new QuarkColumn("fname", "string"));
      add(new QuarkColumn("mi", "string"));
      add(new QuarkColumn("address1", "string"));
      add(new QuarkColumn("address2", "string"));
      add(new QuarkColumn("address3", "string"));
      add(new QuarkColumn("address4", "string"));
      add(new QuarkColumn("city", "string"));
      add(new QuarkColumn("state_province", "string"));
      add(new QuarkColumn("postal_code", "string"));
      add(new QuarkColumn("country", "string"));
      add(new QuarkColumn("customer_region", "string"));
      add(new QuarkColumn("phone1", "string"));
      add(new QuarkColumn("phone2", "string"));
      add(new QuarkColumn("birthdate", "date"));
      add(new QuarkColumn("marital_status", "string"));
      add(new QuarkColumn("yearly_income", "string"));
      add(new QuarkColumn("gender", "string"));
      add(new QuarkColumn("total_children", "int"));
      add(new QuarkColumn("num_children_at_home", "int"));
      add(new QuarkColumn("education", "string"));
      add(new QuarkColumn("date_accnt_opened", "date"));
      add(new QuarkColumn("member_card", "string"));
      add(new QuarkColumn("occupation", "string"));
      add(new QuarkColumn("houseowner", "string"));
      add(new QuarkColumn("num_cars_owned", "int"));
      add(new QuarkColumn("full_name", "string"));
    }});
    builder.put("CUSTOMER", customer);

    QuarkTable days = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("day", "int"));
      add(new QuarkColumn("week_day", "string"));
    }});
    builder.put("DAYS", days);

    QuarkTable department = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("department_id", "int"));
      add(new QuarkColumn("department_description", "string"));
    }});
    builder.put("DEPARTMENT", department);

    QuarkTable employee = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("employee_id", "int"));
      add(new QuarkColumn("full_name", "string"));
      add(new QuarkColumn("first_name", "string"));
      add(new QuarkColumn("last_name", "string"));
      add(new QuarkColumn("position_id", "int"));
      add(new QuarkColumn("position_title", "string"));
      add(new QuarkColumn("store_id", "int"));
      add(new QuarkColumn("department_id", "int"));
      add(new QuarkColumn("birth_date", "date"));
      add(new QuarkColumn("hire_date", "timestamp"));
      add(new QuarkColumn("end_date", "timestamp"));
      add(new QuarkColumn("supervisor_id", "int"));
      add(new QuarkColumn("education_level", "string"));
      add(new QuarkColumn("marital_status", "string"));
      add(new QuarkColumn("gender", "string"));
      add(new QuarkColumn("management_role", "string"));
    }});
    builder.put("EMPLOYEE", employee);

    QuarkTable employee_closure = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("employee_id", "int"));
      add(new QuarkColumn("supervisor_id", "int"));
      add(new QuarkColumn("distance", "int"));
    }});
    builder.put("EMPLOYEE_CLOSURE", employee_closure);

    QuarkTable expense_fact = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("store_id", "int"));
      add(new QuarkColumn("account_id", "int"));
      add(new QuarkColumn("exp_date", "timestamp"));
      add(new QuarkColumn("category_id", "varchar"));
      add(new QuarkColumn("currency_id", "int"));
      add(new QuarkColumn("amount", "double"));
    }});
    builder.put("EXPENSE_FACT", expense_fact);

    QuarkTable position = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("position_id", "int"));
      add(new QuarkColumn("position_title", "string"));
      add(new QuarkColumn("pay_type", "string"));
      add(new QuarkColumn("min_scale", "double"));
      add(new QuarkColumn("max_scale", "double"));
      add(new QuarkColumn("management_role", "string"));
    }});
    builder.put("POSITION", position);

    QuarkTable product = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_class_id", "int"));
      add(new QuarkColumn("product_id", "int"));
      add(new QuarkColumn("brand_name", "string"));
      add(new QuarkColumn("sku", "int"));
      add(new QuarkColumn("srp", "double"));
      add(new QuarkColumn("gross_weight", "double"));
      add(new QuarkColumn("net_weight", "double"));
      add(new QuarkColumn("recyclable_package", "int"));
      add(new QuarkColumn("low_fat", "int"));
      add(new QuarkColumn("units_per_case", "int"));
      add(new QuarkColumn("cases_per_pallet", "int"));
      add(new QuarkColumn("shelf_width", "double"));
      add(new QuarkColumn("shelf_height", "double"));
      add(new QuarkColumn("shelf_depth", "double"));
    }});
    builder.put("PRODUCT", product);

    QuarkTable product_class = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_class_id", "int"));
      add(new QuarkColumn("product_sub_category", "string"));
      add(new QuarkColumn("product_category", "string"));
      add(new QuarkColumn("product_department", "string"));
      add(new QuarkColumn("product_family", "string"));
    }});
    builder.put("PRODUCT_CLASS", product_class);

    QuarkTable promotion = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("promotion_id", "int"));
      add(new QuarkColumn("promotion_district_id", "int"));
      add(new QuarkColumn("promotion_name", "string"));
      add(new QuarkColumn("media_type", "string"));
      add(new QuarkColumn("cost", "double"));
      add(new QuarkColumn("start_date", "datetime"));
      add(new QuarkColumn("end_date", "datetime"));
    }});
    builder.put("PROMOTION", promotion);

    QuarkTable region = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("region_id", "int"));
      add(new QuarkColumn("sales_city", "string"));
      add(new QuarkColumn("sales_state_province", "string"));
      add(new QuarkColumn("sales_district", "string"));
      add(new QuarkColumn("sales_region", "string"));
      add(new QuarkColumn("sales_country", "string"));
      add(new QuarkColumn("sales_district_id", "int"));
    }});
    builder.put("REGION", region);

    QuarkTable reserve_employee = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("employee_id", "int"));
      add(new QuarkColumn("full_name", "string"));
      add(new QuarkColumn("first_name", "string"));
      add(new QuarkColumn("last_name", "string"));
      add(new QuarkColumn("position_id", "int"));
      add(new QuarkColumn("position_title", "string"));
      add(new QuarkColumn("store_id", "int"));
      add(new QuarkColumn("department_id", "int"));
      add(new QuarkColumn("birth_date", "date"));
      add(new QuarkColumn("hire_date", "timestamp"));
      add(new QuarkColumn("end_date", "timestamp"));
      add(new QuarkColumn("supervisor_id", "int"));
      add(new QuarkColumn("education_level", "string"));
      add(new QuarkColumn("marital_status", "string"));
      add(new QuarkColumn("gender", "string"));
      add(new QuarkColumn("management_role", "string"));
    }});
    builder.put("RESERVE_EMPLOYEE", reserve_employee);

    QuarkTable salary = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("pay_date", "timestamp"));
      add(new QuarkColumn("employee_id", "int"));
      add(new QuarkColumn("department_id", "int"));
      add(new QuarkColumn("currency_id", "int"));
      add(new QuarkColumn("salary_paid", "double"));
      add(new QuarkColumn("overtime_paid", "double"));
      add(new QuarkColumn("vacation_accrued", "double"));
      add(new QuarkColumn("vacation_used", "double"));
    }});
    builder.put("SALARY", salary);

    QuarkTable store = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("store_id", "int"));
      add(new QuarkColumn("store_type", "string"));
      add(new QuarkColumn("region_id", "int"));
      add(new QuarkColumn("store_name", "string"));
      add(new QuarkColumn("store_number", "int"));
      add(new QuarkColumn("store_street_address", "string"));
      add(new QuarkColumn("store_city", "string"));
      add(new QuarkColumn("store_state", "string"));
      add(new QuarkColumn("store_postal_code", "string"));
      add(new QuarkColumn("store_country", "string"));
      add(new QuarkColumn("store_manager", "string"));
      add(new QuarkColumn("store_phone", "string"));
      add(new QuarkColumn("store_fax", "string"));
      add(new QuarkColumn("first_opened_date", "timestamp"));
      add(new QuarkColumn("last_remodel_date", "timestamp"));
      add(new QuarkColumn("store_sqft", "int"));
      add(new QuarkColumn("grocery_sqft", "int"));
      add(new QuarkColumn("frozen_sqft", "int"));
      add(new QuarkColumn("meat_sqft", "int"));
      add(new QuarkColumn("coffee_bar", "int"));
      add(new QuarkColumn("video_store", "int"));
      add(new QuarkColumn("salad_bar", "int"));
      add(new QuarkColumn("prepared_food", "int"));
      add(new QuarkColumn("florist", "int"));
    }});
    builder.put("STORE", store);

    QuarkTable sales_fact_1997 = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_id", "int"));
      add(new QuarkColumn("time_id", "int"));
      add(new QuarkColumn("customer_id", "int"));
      add(new QuarkColumn("promotion_id", "int"));
      add(new QuarkColumn("store_id", "int"));
      add(new QuarkColumn("store_sales", "double"));
      add(new QuarkColumn("store_cost", "double"));
      add(new QuarkColumn("unit_sales", "double"));
    }});
    builder.put("SALES_FACT_1997", sales_fact_1997);

    QuarkTable time_by_day = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("time_id", "int"));
      add(new QuarkColumn("the_date", "timestamp"));
      add(new QuarkColumn("the_day", "string"));
      add(new QuarkColumn("the_month", "string"));
      add(new QuarkColumn("the_year", "int"));
      add(new QuarkColumn("day_of_month", "int"));
      add(new QuarkColumn("week_of_year", "int"));
      add(new QuarkColumn("month_of_year", "int"));
      add(new QuarkColumn("quarter", "string"));
      add(new QuarkColumn("fiscal_period", "string"));
    }});
    builder.put("TIME_BY_DAY", time_by_day);

    QuarkTable warehouse = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("warehouse_id", "int"));
      add(new QuarkColumn("warehouse_class_id", "int"));
      add(new QuarkColumn("stores_id", "int"));
      add(new QuarkColumn("warehouse_name", "string"));
      add(new QuarkColumn("wa_address1", "string"));
      add(new QuarkColumn("wa_address2", "string"));
      add(new QuarkColumn("wa_address3", "string"));
      add(new QuarkColumn("wa_address4", "string"));
      add(new QuarkColumn("warehouse_city", "string"));
      add(new QuarkColumn("warehouse_state", "string"));
      add(new QuarkColumn("warehouse_postal_code", "string"));
      add(new QuarkColumn("warehouse_country", "string"));
      add(new QuarkColumn("warehouse_owner_name", "string"));
      add(new QuarkColumn("warehouse_phone", "string"));
      add(new QuarkColumn("warehouse_fax", "string"));
    }});
    builder.put("WAREHOUSE", warehouse);

    QuarkTable warehouse_class = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("warehouse_class_id", "int"));
      add(new QuarkColumn("description", "string"));
    }});
    builder.put("WAREHOUSE_CLASS", warehouse_class);

    QuarkTable inventory_fact = new QuarkTable(new ArrayList<QuarkColumn>() {{
      add(new QuarkColumn("product_id", "int"));
      add(new QuarkColumn("time_id", "int"));
      add(new QuarkColumn("warehouse_id", "int"));
      add(new QuarkColumn("store_id", "int"));
      add(new QuarkColumn("units_ordered", "int"));
      add(new QuarkColumn("units_shipped", "int"));
      add(new QuarkColumn("warehouse_sales", "int"));
      add(new QuarkColumn("supply_time", "int"));
      add(new QuarkColumn("store_invoice", "int"));
    }});
    builder.put("INVENTORY_FACT", inventory_fact);

    QuarkTable count_fact_tile = new QuarkTable(new ImmutableList.Builder<QuarkColumn>()
        .add(new QuarkColumn("the_year", "int"))
        .add(new QuarkColumn("quarter", "string"))
        .add(new QuarkColumn("grouping_id", "string"))
        .add(new QuarkColumn("total_store_sales", "double"))
        .add(new QuarkColumn("total_unit_sales", "double")).build()
    );
    builder.put("COUNT_FACT_TILE", count_fact_tile);

    return builder.build();
  }
}
