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
    SqlNode source;
    SqlNodeList columnList = null;
    SqlParserPos pos;
}
{
    <CREATE>
    {
        pos = getPos();
    }
    <DATASOURCE>
    [
        LOOKAHEAD(2)
        columnList = ParenthesizedSimpleIdentifierList()
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateQuarkDataSource(pos, source, columnList);
    }
}

/**
 * Parses an ALTER DATASOURCE statement.
 */
SqlNode SqlAlterQuarkDataSource() :
{
    SqlNode condition;
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
    condition = WhereOpt()
    {
        return new SqlAlterQuarkDataSource(pos, targetColumnList, sourceExpressionList,
            condition);
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
    <DROP> <DATASOURCE>
    {
        pos = getPos();
    }
    condition = WhereOpt()
    {
        return new SqlDropQuarkDataSource(pos, condition);
    }
}

/**
 * Parses an CREATE VIEW statement.
 */
SqlNode SqlCreateQuarkView() :
{
    SqlNode source;
    SqlNodeList columnList = null;
    SqlParserPos pos;
}
{
    <CREATE>
    {
        pos = getPos();
    }
    <VIEW>
    [
        LOOKAHEAD(2)
        columnList = ParenthesizedSimpleIdentifierList()
    ]
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateQuarkView(pos, source, columnList);
    }
}

/**
 * Parses an ALTER VIEW statement.
 */
SqlNode SqlAlterQuarkView() :
{
    SqlNode condition;
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
    condition = WhereOpt()
    {
        return new SqlAlterQuarkView(pos, targetColumnList, sourceExpressionList,
            condition);
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
    <DROP> <VIEW>
    {
        pos = getPos();
    }
    condition = WhereOpt()
    {
        return new SqlDropQuarkView(pos, condition);
    }
}

/**
 * Parses a SHOW DDL statement.
 */
SqlNode SqlShowQuark() :
{
    SqlNode condition;
    SqlParserPos pos;
    SqlIdentifier quarkEntity;
}
{
    <SHOW>
    {
        pos = getPos();
    }
    (
        <DATASOURCE>
        {
            quarkEntity = new SqlIdentifier("DATASOURCE", getPos());
        }
        |
        <VIEW>
        {
            quarkEntity = new SqlIdentifier("VIEW", getPos());
        }
    )
    condition = WhereOpt()
    {
        return new SqlShowQuark(pos, quarkEntity, condition);
    }
}

