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

package com.qubole.quark.fatjdbc.impl;

import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalcitePrepare;

import com.qubole.quark.fatjdbc.QuarkConnectionImpl;

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;

/**
 * Implementation of prepared statement for JDBC 4.1.
 */
public class PreparedStatementImpl
    extends AvaticaPreparedStatement {
  PreparedStatementImpl(QuarkConnectionImpl connection,
                        Meta.StatementHandle h, CalcitePrepare.CalciteSignature signature,
                        int resultSetType, int resultSetConcurrency,
                        int resultSetHoldability)
      throws SQLException {
    super(connection, h, signature, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public QuarkConnectionImpl getConnection() {
    return (QuarkConnectionImpl) super.getConnection();
  }

  public void setRowId(
      int parameterIndex,
      RowId x) throws SQLException {
    getSite(parameterIndex).setRowId(x);
  }

  public void setNString(
      int parameterIndex, String value) throws SQLException {
    getSite(parameterIndex).setNString(value);
  }

  public void setNCharacterStream(
      int parameterIndex,
      Reader value,
      long length) throws SQLException {
    getSite(parameterIndex)
        .setNCharacterStream(value, length);
  }

  public void setNClob(
      int parameterIndex,
      NClob value) throws SQLException {
    getSite(parameterIndex).setNClob(value);
  }

  public void setClob(
      int parameterIndex,
      Reader reader,
      long length) throws SQLException {
    getSite(parameterIndex)
        .setClob(reader, length);
  }

  public void setBlob(
      int parameterIndex,
      InputStream inputStream,
      long length) throws SQLException {
    getSite(parameterIndex)
        .setBlob(inputStream, length);
  }

  public void setNClob(
      int parameterIndex,
      Reader reader,
      long length) throws SQLException {
    getSite(parameterIndex).setNClob(reader, length);
  }

  public void setSQLXML(
      int parameterIndex, SQLXML xmlObject) throws SQLException {
    getSite(parameterIndex).setSQLXML(xmlObject);
  }

  public void setAsciiStream(
      int parameterIndex,
      InputStream x,
      long length) throws SQLException {
    getSite(parameterIndex)
        .setAsciiStream(x, length);
  }

  public void setBinaryStream(
      int parameterIndex,
      InputStream x,
      long length) throws SQLException {
    getSite(parameterIndex)
        .setBinaryStream(x, length);
  }

  public void setCharacterStream(
      int parameterIndex,
      Reader reader,
      long length) throws SQLException {
    getSite(parameterIndex)
        .setCharacterStream(reader, length);
  }

  public void setAsciiStream(
      int parameterIndex, InputStream x) throws SQLException {
    getSite(parameterIndex).setAsciiStream(x);
  }

  public void setBinaryStream(
      int parameterIndex, InputStream x) throws SQLException {
    getSite(parameterIndex).setBinaryStream(x);
  }

  public void setCharacterStream(
      int parameterIndex, Reader reader) throws SQLException {
    getSite(parameterIndex)
        .setCharacterStream(reader);
  }

  public void setNCharacterStream(
      int parameterIndex, Reader value) throws SQLException {
    getSite(parameterIndex)
        .setNCharacterStream(value);
  }

  public void setClob(
      int parameterIndex,
      Reader reader) throws SQLException {
    getSite(parameterIndex).setClob(reader);
  }

  public void setBlob(
      int parameterIndex, InputStream inputStream) throws SQLException {
    getSite(parameterIndex)
        .setBlob(inputStream);
  }

  public void setNClob(
      int parameterIndex, Reader reader) throws SQLException {
    getSite(parameterIndex)
        .setNClob(reader);
  }
}
