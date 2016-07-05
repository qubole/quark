<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#--
  Add implementations of additional parser statements here.
  Each implementation should return an object of SqlNode type.

  Example of SqlShowTables() implementation:
  SqlNode SqlShowTables()
  {
    ...local variables...
  }
  {
    <SHOW> <TABLES>
    ...
    {
      return SqlShowTables(...)
    }
  }
-->

/**
 * Parses an CREATE DATASOURCE statement.
 */
SqlNode SqlCreateQuarkDataSource() :
{
    SqlIdentifier identifier;
    SqlNodeList sourceExpressionList;
    SqlNodeList targetColumnList;
    SqlIdentifier id;
    SqlNode exp;
    SqlParserPos pos;
}
{
    <CREATE>
    <DATASOURCE>
    {
        pos = getPos();
        targetColumnList = new SqlNodeList(pos);
        sourceExpressionList = new SqlNodeList(pos);
    }
    identifier = SimpleIdentifier()
    <SET> id = SimpleIdentifier()
    {
        targetColumnList.add(id);
    }
    <EQ> exp = Expression(ExprContext.ACCEPT_SUBQUERY)
    {
        sourceExpressionList.add(exp);
    }
    (
        <COMMA>
        id = SimpleIdentifier()
        {
            targetColumnList.add(id);
        }
        <EQ> exp = Expression(ExprContext.ACCEPT_SUBQUERY)
        {
            sourceExpressionList.add(exp);
        }
    ) *
    {
        return new SqlCreateQuarkDataSource(pos, targetColumnList, sourceExpressionList,
            identifier);
    }
}

/**
 * Parses an ALTER DATASOURCE statement.
 */
SqlNode SqlAlterQuarkDataSource() :
{
    SqlIdentifier identifier;
    SqlNodeList sourceExpressionList;
    SqlNodeList targetColumnList;
    SqlIdentifier id;
    SqlNode exp;
    SqlParserPos pos;
}
{
    <ALTER> <DATASOURCE>
    {
        pos = getPos();
        targetColumnList = new SqlNodeList(pos);
        sourceExpressionList = new SqlNodeList(pos);
    }
    identifier = SimpleIdentifier()
    <SET> id = SimpleIdentifier()
    {
        targetColumnList.add(id);
    }
    <EQ> exp = Expression(ExprContext.ACCEPT_SUBQUERY)
    {
        sourceExpressionList.add(exp);
    }
    (
        <COMMA>
        id = SimpleIdentifier()
        {
            targetColumnList.add(id);
        }
        <EQ> exp = Expression(ExprContext.ACCEPT_SUBQUERY)
        {
            sourceExpressionList.add(exp);
        }
    ) *
    {
        return new SqlAlterQuarkDataSource(pos, targetColumnList, sourceExpressionList,
            identifier);
    }
}

/**
 * Parses a DELETE statement.
 */
SqlNode SqlDropQuarkDataSource() :
{
    SqlNode condition;
    SqlParserPos pos;
}
{
    <DROP> { pos = getPos(); }
    <DATASOURCE>
    {
        return new SqlDropQuarkDataSource(pos, CompoundIdentifier());
    }
}

/**
 * Parses a create view or replace existing view statement.
 *   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlNode SqlCreateQuarkView() :
{
    SqlParserPos pos;
    SqlIdentifier viewName;
    SqlIdentifier tableName;
    SqlNode query;
}
{
    <CREATE> <VIEW>
    { pos = getPos(); }
    viewName = SimpleIdentifier()
    <STORED> <IN>
    tableName = CompoundIdentifier()
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateQuarkView(pos, viewName, tableName, query);
    }
}

/**
 * Parses an ALTER VIEW statement.
 */
SqlNode SqlAlterQuarkView() :
{
    SqlIdentifier identifier;
    SqlNodeList sourceExpressionList;
    SqlNodeList targetColumnList;
    SqlIdentifier id;
    SqlNode exp;
    SqlParserPos pos;
}
{
    <ALTER> <VIEW>
    {
        pos = getPos();
        targetColumnList = new SqlNodeList(pos);
        sourceExpressionList = new SqlNodeList(pos);
    }
    identifier = SimpleIdentifier()
    <SET> id = SimpleIdentifier()
    {
        targetColumnList.add(id);
    }
    <EQ> exp = Expression(ExprContext.ACCEPT_SUBQUERY)
    {
        sourceExpressionList.add(exp);
    }
    (
        <COMMA>
        id = SimpleIdentifier()
        {
            targetColumnList.add(id);
        }
        <EQ> exp = Expression(ExprContext.ACCEPT_SUBQUERY)
        {
            sourceExpressionList.add(exp);
        }
    ) *
    {
        return new SqlAlterQuarkView(pos, targetColumnList, sourceExpressionList,
            identifier);
    }
}

/**
 * Parses a DELETE VIEW statement.
 */
SqlNode SqlDropQuarkView() :
{
    SqlNode condition;
    SqlParserPos pos;
}
{
    <DROP> { pos = getPos(); }
    <VIEW>
    {
        return new SqlDropQuarkView(pos, CompoundIdentifier());
    }
}

/**
 * Parses a SHOW DDL statement.
 */
SqlNode SqlShowDataSources() :
{
    SqlParserPos pos;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    <DATASOURCE>
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowDataSources(pos, likePattern);
    }
}

SqlNode SqlShowViews() :
{
    SqlParserPos pos;
    SqlNode likePattern = null;
}
{
    <SHOW> { pos = getPos(); }
    <VIEW>
    [
        <LIKE> { likePattern = StringLiteral(); }
    ]
    {
        return new SqlShowViews(pos, likePattern);
    }
}
