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
package com.qubole.quark.planner.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import com.qubole.quark.QuarkException;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by adeshr on 5/24/16.
 */
public class ParserFactory {
  private SqlQueryParser sqlQueryParser;
  private boolean reloadCache;

  public void setReloadCache() {
    this.reloadCache = true;
  }

  public void clearReloadCache() {
    this.reloadCache = false;
  }

  public SqlQueryParser getSqlQueryParser(Properties info, boolean reloadCache)
      throws SQLException {
    if (reloadCache || sqlQueryParser == null) {
      try {
        sqlQueryParser = new SqlQueryParser(info);
        clearReloadCache();
      } catch (QuarkException e) {
        throw new SQLException(e.getMessage(), e);
      }
    }
    return sqlQueryParser;
  }

  public Parser getParser(String sql, Properties info, boolean reloadCache)
      throws SQLException {
    SqlParser parser = SqlParser.create(sql,
        SqlParser.configBuilder()
            .setQuotedCasing(Casing.UNCHANGED)
            .setUnquotedCasing(Casing.UNCHANGED)
            .setQuoting(Quoting.DOUBLE_QUOTE)
            .build());
    SqlNode sqlNode;
    try {
      sqlNode = parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException(
          "parse failed: " + e.getMessage(), e);
    }
    if (sqlNode.getKind().equals(SqlKind.OTHER_DDL)) {
      return new DDLParser();
    } else  {
      return getSqlQueryParser(info, reloadCache);
    }
  }
}
