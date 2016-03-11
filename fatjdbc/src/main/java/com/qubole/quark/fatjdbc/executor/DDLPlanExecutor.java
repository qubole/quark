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
package com.qubole.quark.fatjdbc.executor;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlAlterQuark;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreateQuark;
import org.apache.calcite.sql.SqlDropQuark;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import com.qubole.quark.catalog.db.dao.DataSourceDAO;
import com.qubole.quark.catalog.db.dao.JdbcSourceDAO;
import com.qubole.quark.catalog.db.dao.QuboleDbSourceDAO;
import com.qubole.quark.catalog.db.pojo.DataSource;
import com.qubole.quark.catalog.db.pojo.JdbcSource;
import com.qubole.quark.catalog.db.pojo.QuboleDbSource;
import com.qubole.quark.fatjdbc.QuarkConnectionImpl;
import com.qubole.quark.fatjdbc.QuarkMetaResultSet;
import com.qubole.quark.planner.parser.ParserResult;

import org.skife.jdbi.v2.DBI;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by amoghm on 3/4/16.
 */
public class DDLPlanExecutor implements PlanExecutor {
  Meta.StatementHandle h;
  QuarkConnectionImpl connection;
  DBI dbi = null;

  DDLPlanExecutor(Meta.StatementHandle h, QuarkConnectionImpl connection) {
    this.h = h;
    this.connection = connection;
  }

  public QuarkMetaResultSet execute(ParserResult result)
      throws Exception {
    SqlParser parser = SqlParser.create(result.getParsedSql(),
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
    if (sqlNode instanceof SqlCreateQuark) {
      int id = executeCreateDataSource((SqlCreateQuark) sqlNode);
      connection.resetSelectParser();
      return QuarkMetaResultSet.count(h.connectionId, h.id, id);
    } else if (sqlNode instanceof SqlAlterQuark) {
      int id = executeAlterDataSource((SqlAlterQuark) sqlNode);
      connection.resetSelectParser();
      return QuarkMetaResultSet.count(h.connectionId, h.id, id);
    } else if (sqlNode instanceof SqlDropQuark) {
      executeDeleteOnDataSource((SqlDropQuark) sqlNode);
      connection.resetSelectParser();
      return QuarkMetaResultSet.count(h.connectionId, h.id, 0);
    }
    throw new RuntimeException("Cannot handle execution for: " + result.getParsedSql());
  }

  private int parseCondition(SqlNode cond) throws SQLException {
    if (cond instanceof SqlBasicCall) {
      final SqlBasicCall condCall = (SqlBasicCall) cond;
      if (condCall.getOperator().getKind() == SqlKind.EQUALS) {
        if (condCall.getOperandList().size() == 2) {
          if (condCall.operand(0) instanceof SqlIdentifier
              && condCall.operand(1) instanceof SqlNumericLiteral) {
            if (((SqlIdentifier) condCall.operand(0)).getSimple().equals("id")) {
              return ((SqlNumericLiteral) condCall.operand(1)).intValue(true);
            }
          } else if (condCall.operand(1) instanceof SqlIdentifier
              && condCall.operand(0) instanceof SqlNumericLiteral) {
            if (((SqlIdentifier) condCall.operand(1)).getSimple().equals("id")) {
              return ((SqlNumericLiteral) condCall.operand(0)).intValue(true);
            }
          }
        }
      }
    }
    throw new SQLException("Only condition supported by ALTER DATASOURCE is"
        + " 'id = <constant>'");
  }

  public int executeAlterDataSource(SqlAlterQuark sqlNode) throws SQLException {
    int idToUpdate = parseCondition(sqlNode.getCondition());
    DBI dbi = getDBI();
    DataSourceDAO dataSourceDAO = dbi.onDemand(DataSourceDAO.class);
    JdbcSourceDAO jdbcDAO = dbi.onDemand(JdbcSourceDAO.class);
    QuboleDbSourceDAO quboleDAO = dbi.onDemand(QuboleDbSourceDAO.class);
    DataSource dataSource = jdbcDAO.find(idToUpdate);
    if (dataSource == null) {
      dataSource = quboleDAO.find(idToUpdate);
    }
    if (dataSource == null) {
      return 0;
    }
    SqlNodeList rowList = sqlNode.getSourceExpressionList();
    int i = 0;
    for (SqlNode node : sqlNode.getTargetColumnList()) {
      if (node instanceof SqlIdentifier) {
        switch (((SqlIdentifier) node).getSimple()) {
          case "name":
            dataSource.setName(rowList.get(i).toString());
            break;
          case "type":
            dataSource.setType(rowList.get(i).toString());
            break;
          case "url":
            dataSource.setUrl(rowList.get(i).toString());
            break;
          case "ds_set_id":
            if (rowList.get(i) instanceof SqlNumericLiteral) {
              dataSource.setDsSetId(((SqlNumericLiteral) rowList.get(i)).longValue(true));
            } else {
              throw new SQLException("Incorrect argument type to variable 'ds_set_id'");
            }
            break;
          case "datasource_type":
            dataSource.setDatasourceType(rowList.get(i).toString());
            break;
          case "username":
            if (dataSource instanceof JdbcSource) {
              ((JdbcSource) dataSource)
                  .setUsername(rowList.get(i).toString());
            }
            break;
          case "password":
            if (dataSource instanceof JdbcSource) {
              ((JdbcSource) dataSource)
                  .setPassword(rowList.get(i).toString());
            }
            break;
          case "dbtap_id":
            if (dataSource instanceof QuboleDbSource) {
              if (rowList.get(i) instanceof SqlNumericLiteral) {
                ((QuboleDbSource) dataSource).setDbTapId(
                    ((SqlNumericLiteral) rowList.get(i)).intValue(true));
              } else {
                throw new SQLException("Incorrect argument type to variable"
                    + " 'dbtap_id'");
              }
            }
            break;
          case "auth_token":
            if (dataSource instanceof QuboleDbSource) {
              ((QuboleDbSource) dataSource)
                  .setAuthToken(rowList.get(i).toString());
            }
            break;
          default:
            throw new SQLException("Unknown parameter: " + ((SqlIdentifier) node).getSimple());
        }
        i++;
      }
    }

    if (dataSource instanceof JdbcSource) {
      return jdbcDAO.update((JdbcSource) dataSource, dataSourceDAO,
          connection.getProperties().getProperty("encryptionKey"));
    } else {
      return quboleDAO.update((QuboleDbSource) dataSource, dataSourceDAO,
          connection.getProperties().getProperty("encryptionKey"));
    }
  }

  private DBI getDBI() {
    Properties info = connection.getProperties();
    return new DBI(
          info.getProperty("url"),
          info.getProperty("user"),
          info.getProperty("password"));
  }

  public int executeCreateDataSource(SqlCreateQuark sqlNode) throws SQLException {
    DBI dbi = getDBI();
    Map<String, Object> commonColumns = new HashMap<>();
    Map<String, Object> dbSpecificColumns = new HashMap<>();
    DataSourceDAO dataSourceDAO = dbi.onDemand(DataSourceDAO.class);
    JdbcSourceDAO jdbcSourceDAO = null;
    QuboleDbSourceDAO quboleDbSourceDAO = null;
    SqlNode source = sqlNode.getSource();
    if (source instanceof SqlBasicCall) {
      SqlBasicCall rowCall = ((SqlBasicCall) source).operand(0);
      int i = 0;
      for (SqlNode node : sqlNode.getTargetColumnList()) {
        if (node instanceof SqlIdentifier) {
          switch (((SqlIdentifier) node).getSimple()) {
            case "name":
              commonColumns.put("name", rowCall.operand(i).toString());
              break;
            case "type":
              commonColumns.put("type", rowCall.operand(i).toString());
              break;
            case "url":
              commonColumns.put("url", rowCall.operand(i).toString());
              break;
            case "ds_set_id":
              if (rowCall.operand(i) instanceof SqlNumericLiteral) {
                commonColumns.put("ds_set_id",
                    ((SqlNumericLiteral) rowCall.operand(i)).longValue(true));
              } else {
                throw new SQLException("Incorrect argument type to variable 'ds_set_id'");
              }
              break;
            case "datasource_type":
              if (rowCall.operand(i).toString().toUpperCase().equals("JDBC")) {
                jdbcSourceDAO = dbi.onDemand(JdbcSourceDAO.class);
              } else if (rowCall.operand(i).toString().toUpperCase().equals("QUBOLEDB")) {
                quboleDbSourceDAO = dbi.onDemand(QuboleDbSourceDAO.class);
              } else {
                throw new SQLException("Incorrect argument type to variable"
                    + " 'datasource_type'");
              }
              commonColumns.put("datasource_type", rowCall.operand(i).toString());
              break;
            case "username":
              dbSpecificColumns.put("username", rowCall.operand(i).toString());
              break;
            case "password":
              dbSpecificColumns.put("password", rowCall.operand(i).toString());
              break;
            case "dbtap_id":
              if (rowCall.operand(i) instanceof SqlNumericLiteral) {
                dbSpecificColumns.put("dbtap_id",
                    ((SqlNumericLiteral) rowCall.operand(i)).intValue(true));
              } else {
                throw new SQLException("Incorrect argument type to variable"
                    + " 'dbtap_id'");
              }
              break;
            case "auth_token":
              dbSpecificColumns.put("auth_token", rowCall.operand(i).toString());
              break;
            default:
              throw new SQLException("Unknown parameter: " + ((SqlIdentifier) node).getSimple());
          }
        } else {
          throw new RuntimeException("Error in parsing the DDL "
              + "statement to create DataSource");
        }
        i++;
      }
      if ((jdbcSourceDAO == null && quboleDbSourceDAO == null)
          || (jdbcSourceDAO != null && quboleDbSourceDAO != null)) {
        throw new RuntimeException("Need to pass exact values to create"
            + " data source of type jdbc or quboleDb");
      } else if (jdbcSourceDAO != null) {
        return dataSourceDAO.insertJDBC((String) commonColumns.get("name"),
            (String) commonColumns.get("type"),
            (String) commonColumns.get("url"),
            (long) commonColumns.get("ds_set_id"),
            (String) commonColumns.get("datasource_type"),
            jdbcSourceDAO,
            (String) dbSpecificColumns.get("username"),
            (dbSpecificColumns.get("password") == null) ? ""
                : (String) dbSpecificColumns.get("password"),
            connection.getProperties().getProperty("encryptionKey"));
      } else {
        return dataSourceDAO.insertQuboleDB((String) commonColumns.get("name"),
            (String) commonColumns.get("type"),
            (String) commonColumns.get("url"),
            (long) commonColumns.get("ds_set_id"),
            (String) commonColumns.get("datasource_type"),
            quboleDbSourceDAO,
            (int) dbSpecificColumns.get("dbtap_id"),
            (String) dbSpecificColumns.get("auth_token"),
            connection.getProperties().getProperty("encryptionKey"));
      }
    } else {
      throw new RuntimeException("Incorrect DDL Statement to create Datasources");
    }
  }
  private void executeDeleteOnDataSource(SqlDropQuark node) throws SQLException {
    int id = parseCondition(node.getCondition());
    DBI dbi = getDBI();
    DataSourceDAO dataSourceDAO = dbi.onDemand(DataSourceDAO.class);
    JdbcSourceDAO jdbcDao = dbi.onDemand(JdbcSourceDAO.class);
    QuboleDbSourceDAO quboleDao = dbi.onDemand(QuboleDbSourceDAO.class);
    jdbcDao.delete(id);
    quboleDao.delete(id);
    dataSourceDAO.delete(id);
  }
}
