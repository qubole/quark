package com.qubole.quark.planner.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.qubole.quark.QuarkException;
import com.qubole.quark.planner.MetadataSchema;
import com.qubole.quark.planner.QuarkSchema;
import com.qubole.quark.planner.QuarkView;
import com.qubole.quark.planner.TestFactory;
import com.qubole.quark.planner.parser.SqlQueryParser;
import com.qubole.quark.planner.test.utilities.QuarkTestUtil;
import com.qubole.quark.sql.QueryContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by qubole on 4/27/16.
 */
public class ViewsTest {
  private static final Logger log = LoggerFactory.getLogger(ViewsTest.class);
  private static SqlQueryParser parser;
  private static Properties info;

  public static class ViewSchema extends MetadataSchema {
    ViewSchema() {}

    public QuarkView webSitePartition() {
      final QuarkView view = new QuarkView("web_site_partition", "select web_site_sk, " +
              "web_rec_start_date, web_county, web_tax_percentage from tpcds.web_site"
             +  " where web_rec_start_date > '2015-06-29' AND ((web_county = 'en') or "
             +  "(web_county = 'fr') or (web_county = 'ja') or (web_county = 'de') or "
             + "(web_county = 'ru'))", "WEB_SITE_PARTITION",
              ImmutableList.of("TPCDS"), ImmutableList.of("TPCDS", "WEB_SITE_PARTITION"));

      return view;
    }

    @Override
    public void initialize(QueryContext queryContext) throws QuarkException {
      this.views = ImmutableList.of(webSitePartition());
      this.cubes = ImmutableList.of();
      super.initialize(queryContext);
    }
  }

  public static class SchemaFactory extends TestFactory {
    public SchemaFactory() {
      super(new Tpcds("tpcds".toUpperCase()));
    }
    public List<QuarkSchema> create(Properties info) {
      ViewSchema cubeSchema = new ViewSchema();
      return new ImmutableList.Builder<QuarkSchema>()
          .add(this.getDefaultSchema())
          .add(cubeSchema).build();
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    info = new Properties();
    info.put("unitTestMode", "true");
    info.put("schemaFactory", "com.qubole.quark.planner.test.ViewsTest$SchemaFactory");

    info.put("defaultSchema", QuarkTestUtil.toJson("TPCDS"));
    parser = new SqlQueryParser(info);
  }

  @Test
  public void usingWebSiteGroupOrderQuery() throws QuarkException, SQLException {
    String query = "select web_rec_start_date, sum(web_tax_percentage), " +
            "web_county from tpcds.web_site " +
            "where web_rec_start_date > '2015-06-30' AND ((web_county = 'fr') or (web_county = " +
            "'de')) group by web_rec_start_date, web_county " +
            "order by web_rec_start_date";
    QuarkTestUtil.checkParsedSql(
            query,
            info,
            "SELECT WEB_REC_START_DATE, SUM(WEB_TAX_PERCENTAGE), WEB_COUNTY " +
                "FROM TPCDS.WEB_SITE_PARTITION " +
                "WHERE WEB_REC_START_DATE > DATE '2015-06-30' " +
                "AND (WEB_COUNTY = 'fr' OR WEB_COUNTY = 'de') " +
                "GROUP BY WEB_REC_START_DATE, WEB_COUNTY " +
                "ORDER BY WEB_REC_START_DATE");
  }
}
