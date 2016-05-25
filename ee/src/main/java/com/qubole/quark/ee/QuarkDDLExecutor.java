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
package com.qubole.quark.ee;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlAlterQuarkDataSource;
import org.apache.calcite.sql.SqlAlterQuarkView;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCreateQuarkDataSource;
import org.apache.calcite.sql.SqlCreateQuarkView;
import org.apache.calcite.sql.SqlDropQuarkDataSource;
import org.apache.calcite.sql.SqlDropQuarkView;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlShowQuark;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import com.qubole.quark.catalog.db.dao.DataSourceDAO;
import com.qubole.quark.catalog.db.dao.JdbcSourceDAO;
import com.qubole.quark.catalog.db.dao.QuboleDbSourceDAO;
import com.qubole.quark.catalog.db.dao.ViewDAO;
import com.qubole.quark.catalog.db.encryption.AESEncrypt;
import com.qubole.quark.catalog.db.encryption.Encrypt;
import com.qubole.quark.catalog.db.encryption.NoopEncrypt;
import com.qubole.quark.catalog.db.pojo.DataSource;
import com.qubole.quark.catalog.db.pojo.JdbcSource;
import com.qubole.quark.catalog.db.pojo.QuboleDbSource;
import com.qubole.quark.catalog.db.pojo.View;
import com.qubole.quark.planner.parser.ParserFactory;
import com.qubole.quark.planner.parser.ParserResult;

import org.skife.jdbi.v2.DBI;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adeshr on 5/24/16.
 */
public class QuarkDDLExecutor implements QuarkExecutor {
  private Properties info;
  private ParserFactory parserFactory;

  public QuarkDDLExecutor(ParserFactory parserFactory, Properties info) {
    this.parserFactory = parserFactory;
    this.info = info;
  }

  private DBI getDBI() {
    DBI dbi = new DBI(
        info.getProperty("url"),
        info.getProperty("user"),
        info.getProperty("password"));

    if (Boolean.parseBoolean(info.getProperty("encrypt", "false"))) {
      dbi.define("encryptClass", new AESEncrypt(info.getProperty("encryptionKey")));
    } else {
      dbi.define("encryptClass", new NoopEncrypt());
    }

    return dbi;
  }

  public Object execute(ParserResult result) throws SQLException {
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
    if (sqlNode instanceof SqlCreateQuarkDataSource) {
      int id = executeCreateDataSource((SqlCreateQuarkDataSource) sqlNode);
      parserFactory.setReloadCache();
      return id;
    } else if (sqlNode instanceof SqlAlterQuarkDataSource) {
      int id = executeAlterDataSource((SqlAlterQuarkDataSource) sqlNode);
      parserFactory.setReloadCache();
      return id;
    } else if (sqlNode instanceof SqlDropQuarkDataSource) {
      executeDeleteOnDataSource((SqlDropQuarkDataSource) sqlNode);
      parserFactory.setReloadCache();
      return 0;
    } else if (sqlNode instanceof SqlCreateQuarkView) {
      int id = executeCreateView((SqlCreateQuarkView) sqlNode);
      parserFactory.setReloadCache();
      return id;
    } else if (sqlNode instanceof SqlAlterQuarkView) {
      int id = executeAlterView((SqlAlterQuarkView) sqlNode);
      parserFactory.setReloadCache();
      return id;
    } else if (sqlNode instanceof SqlDropQuarkView) {
      executeDeleteOnView((SqlDropQuarkView) sqlNode);
      parserFactory.setReloadCache();
      return 0;
    } else if (sqlNode instanceof SqlShowQuark) {
      return getListFromDAO((SqlShowQuark) sqlNode);
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

  public int executeAlterDataSource(SqlAlterQuarkDataSource sqlNode) throws SQLException {
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

    Encrypt encrypt;
    if (Boolean.parseBoolean(info.getProperty("encrypt", "false"))) {
      encrypt = new AESEncrypt(info.getProperty("encryptionKey"));
    } else {
      encrypt = new NoopEncrypt();
    }
    if (dataSource instanceof JdbcSource) {
      return jdbcDAO.update((JdbcSource) dataSource, dataSourceDAO, encrypt);
    } else {
      return quboleDAO.update((QuboleDbSource) dataSource, dataSourceDAO, encrypt);
    }
  }

  public int executeCreateDataSource(SqlCreateQuarkDataSource sqlNode) throws SQLException {
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

      Encrypt encrypt;
      if (Boolean.parseBoolean(info.getProperty("encrypt", "false"))) {
        encrypt = new AESEncrypt(info.getProperty("encryptionKey"));
      } else {
        encrypt = new NoopEncrypt();
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
            encrypt);
      } else {
        return dataSourceDAO.insertQuboleDB((String) commonColumns.get("name"),
            (String) commonColumns.get("type"),
            (String) commonColumns.get("url"),
            (long) commonColumns.get("ds_set_id"),
            (String) commonColumns.get("datasource_type"),
            quboleDbSourceDAO,
            (int) dbSpecificColumns.get("dbtap_id"),
            (String) dbSpecificColumns.get("auth_token"),
            encrypt);
      }
    } else {
      throw new RuntimeException("Incorrect DDL Statement to create Datasources");
    }
  }
  private void executeDeleteOnDataSource(SqlDropQuarkDataSource node) throws SQLException {
    int id = parseCondition(node.getCondition());
    DBI dbi = getDBI();
    DataSourceDAO dataSourceDAO = dbi.onDemand(DataSourceDAO.class);
    JdbcSourceDAO jdbcDao = dbi.onDemand(JdbcSourceDAO.class);
    QuboleDbSourceDAO quboleDao = dbi.onDemand(QuboleDbSourceDAO.class);
    jdbcDao.delete(id);
    quboleDao.delete(id);
    dataSourceDAO.delete(id);
  }

  public int executeAlterView(SqlAlterQuarkView sqlNode) throws SQLException {
    int idToUpdate = parseCondition(sqlNode.getCondition());
    DBI dbi = getDBI();
    ViewDAO viewDAO = dbi.onDemand(ViewDAO.class);

    View view = viewDAO.find(idToUpdate);
    if (view == null) {
      return 0;
    }

    SqlNodeList rowList = sqlNode.getSourceExpressionList();
    int i = 0;
    for (SqlNode node : sqlNode.getTargetColumnList()) {
      if (node instanceof SqlIdentifier) {
        switch (((SqlIdentifier) node).getSimple()) {
          case "name":
            view.setName(rowList.get(i).toString());
            break;
          case "description":
            view.setDescription(rowList.get(i).toString());
            break;
          case "query":
            view.setQuery(rowList.get(i).toString());
            break;
          case "schema_name":
            view.setSchema(rowList.get(i).toString());
            break;
          case "table_name":
            view.setTable(rowList.get(i).toString());
            break;
          case "ds_set_id":
            if (rowList.get(i) instanceof SqlNumericLiteral) {
              view.setDsSetId(((SqlNumericLiteral) rowList.get(i)).longValue(true));
            } else {
              throw new SQLException("Incorrect argument type to variable 'ds_set_id'");
            }
            break;
          case "cost":
            if (rowList.get(i) instanceof SqlNumericLiteral) {
              view.setCost(((SqlNumericLiteral) rowList.get(i)).longValue(true));
            } else {
              throw new SQLException("Incorrect argument type to variable 'cost'");
            }
            break;
          case "destination_id":
            if (rowList.get(i) instanceof SqlNumericLiteral) {
              view.setDestinationId(((SqlNumericLiteral) rowList.get(i)).longValue(true));
            } else {
              throw new SQLException("Incorrect argument type to variable 'destination_id'");
            }
            break;
          default:
            throw new SQLException("Unknown parameter: " + ((SqlIdentifier) node).getSimple());
        }
        i++;
      }
    }

    return viewDAO.update(view);
  }

  public int executeCreateView(SqlCreateQuarkView sqlNode) throws SQLException {
    DBI dbi = getDBI();
    Map<String, Object> columns = new HashMap<>();
    ViewDAO viewDAO = dbi.onDemand(ViewDAO.class);

    SqlNode source = sqlNode.getSource();
    if (source instanceof SqlBasicCall) {
      SqlBasicCall rowCall = ((SqlBasicCall) source).operand(0);
      int i = 0;
      for (SqlNode node : sqlNode.getTargetColumnList()) {
        if (node instanceof SqlIdentifier) {
          switch (((SqlIdentifier) node).getSimple()) {
            case "name":
              columns.put("name", rowCall.operand(i).toString());
              break;
            case "description":
              columns.put("description", rowCall.operand(i).toString());
              break;
            case "query":
              columns.put("query", rowCall.operand(i).toString());
              break;
            case "schema_name":
              columns.put("schema_name", rowCall.operand(i).toString());
              break;
            case "table_name":
              columns.put("table_name", rowCall.operand(i).toString());
              break;
            case "ds_set_id":
              if (rowCall.operand(i) instanceof SqlNumericLiteral) {
                columns.put("ds_set_id",
                    ((SqlNumericLiteral) rowCall.operand(i)).longValue(true));
              } else {
                throw new SQLException("Incorrect argument type to variable 'ds_set_id'");
              }
              break;
            case "cost":
              if (rowCall.operand(i) instanceof SqlNumericLiteral) {
                columns.put("cost",
                    ((SqlNumericLiteral) rowCall.operand(i)).longValue(true));
              } else {
                throw new SQLException("Incorrect argument type to variable 'cost'");
              }
              break;
            case "destination_id":
              if (rowCall.operand(i) instanceof SqlNumericLiteral) {
                columns.put("destination_id",
                    ((SqlNumericLiteral) rowCall.operand(i)).longValue(true));
              } else {
                throw new SQLException("Incorrect argument type to variable 'destination_id'");
              }
              break;
            default:
              throw new SQLException("Unknown parameter: " + ((SqlIdentifier) node).getSimple());
          }
        } else {
          throw new RuntimeException("Error in parsing the DDL "
              + "statement to create View");
        }
        i++;
      }

      return viewDAO.insert((String) columns.get("name"), (String) columns.get("description"),
          (String) columns.get("query"), (long) columns.get("cost"),
          (long) columns.get("destination_id"), (String) columns.get("schema_name"),
          (String) columns.get("table_name"), (long) columns.get("ds_set_id"));
    } else {
      throw new RuntimeException("Incorrect DDL Statement to create View");
    }
  }

  private void executeDeleteOnView(SqlDropQuarkView node) throws SQLException {
    int id = parseCondition(node.getCondition());
    DBI dbi = getDBI();
    ViewDAO viewDAO = dbi.onDemand(ViewDAO.class);
    viewDAO.delete(id);
  }

  private List getListFromDAO(SqlShowQuark sqlNode) throws SQLException {
    DBI dbi = getDBI();
    String pojoType = sqlNode.getOperator().toString();
    String whereClause = (sqlNode.getCondition() == null)
        ? "" : "where " + sqlNode.getCondition();

    switch (pojoType) {
      case "SHOW_DATASOURCE":
        return getDataSourceList(whereClause, dbi);
      case "SHOW_VIEW":
        ViewDAO viewDAO = dbi.onDemand(ViewDAO.class);
        return viewDAO.findByWhere(whereClause);
      default:
        throw new SQLException("Not supported for: " + pojoType);
    }
  }

  private List<DataSource> getDataSourceList(String whereClause, DBI dbi) {

    boolean isQuboleDBQuery =
        whereClause.contains(" `auth_token`") || whereClause.contains(" `dbtap_id`");
    boolean isJDBCDBQuery =
        whereClause.contains(" `username`") || whereClause.contains(" `password`");

    whereClause = preProcessWhereClause(whereClause);

    JdbcSourceDAO jdbcSourceDAO = dbi.onDemand(JdbcSourceDAO.class);
    QuboleDbSourceDAO quboleDbSourceDAO = dbi.onDemand(QuboleDbSourceDAO.class);
    List<DataSource> dataSources = new ArrayList<>();

    if (isJDBCDBQuery) {
      dataSources.addAll(jdbcSourceDAO.findByWhere(whereClause));
    } else if (isQuboleDBQuery) {
      dataSources.addAll(quboleDbSourceDAO.findByWhere(whereClause));
    } else {
      dataSources.addAll(jdbcSourceDAO.findByWhere(whereClause));
      dataSources.addAll(quboleDbSourceDAO.findByWhere(whereClause));
    }
    return dataSources;
  }
  private String preProcessWhereClause(String whereClause) {
    if (whereClause.contains(" `id`")) {
      whereClause = whereClause.replace(" `id`", " data_sources.id");
    }
    whereClause = whereClause.replace(" `username`", " jdbc_sources.username");
    whereClause = whereClause.replace(" `password`", " jdbc_sources.password");
    return whereClause;
  }
}
