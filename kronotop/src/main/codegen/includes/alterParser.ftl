SqlNodeList RepeatedAlterColumnList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    { s = span(); }
    AlterTableColumElement(list)
    (
        <COMMA>
        AlterTableColumElement(list)
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}


// TableElementList without parens
SqlNodeList NoParenTableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}

SqlNodeList DropColumnNodeList() :
{
    SqlIdentifier id;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    [<COLUMN>]
    id = CompoundIdentifier() {list.add(id);}
    (
        <COMMA> [<DROP>][<COLUMN>] id = CompoundIdentifier() {
            list.add(id);
        }
    )*
    { return new SqlNodeList(list, SqlParserPos.ZERO); }
}

void AlterTableColumElement(List<SqlNode> list) :
{
    SqlIdentifier id;
}
{
    <ALTER>
    [<COLUMN>]
    id = CompoundIdentifier()
    [<SET> <DATA>]
    <TYPE>
    TableColumn(id,list)
}

void TableColumn(SqlIdentifier id, List<SqlNode> list) :
{
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode defval;
    SqlIdentifier name = null;
    final Span s = Span.of();
    final ColumnStrategy strategy;
}
{
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        (
            <DEFAULT_> defval = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                strategy = ColumnStrategy.DEFAULT;
            }
        |
            {
                defval = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), defval, strategy));
        }
    |
        { list.add(id); }
    )
}

/*
 * Alter a table using the following syntax:
 *
 * ALTER TABLE <table_name>
 *
 */
SqlDdl SqlAlterTable(Span s) :
{
    final SqlIdentifier name;
    final SqlAlterTable.AlterType alterType;
    SqlIdentifier newTableName = null;
    SqlIdentifier columnName = null;
    SqlIdentifier newColumnName = null;
    SqlIdentifier columnType = null;
    SqlNodeList columnList = null;
}
{
    <ALTER>
    <TABLE>
    name = CompoundIdentifier()
    (
        <RENAME>
        (
            <TO>
            newTableName = SimpleIdentifier()
            {
                alterType = SqlAlterTable.AlterType.RENAME_TABLE;
            }
        |
            <COLUMN>
            columnName = SimpleIdentifier()
            <TO>
            newColumnName = SimpleIdentifier()
            {
                alterType = SqlAlterTable.AlterType.RENAME_COLUMN;
            }
        )
    |
        <DROP>
        columnList = DropColumnNodeList()
        {
            alterType = SqlAlterTable.AlterType.DROP_COLUMN;
        }
    |
        <ADD>
        [<COLUMN>]
        (
            columnList = TableElementList()
            |
            columnList = NoParenTableElementList()
        )
        {
            alterType = SqlAlterTable.AlterType.ADD_COLUMN;
        }
    |
        columnList = RepeatedAlterColumnList()
        {
            alterType = SqlAlterTable.AlterType.ALTER_COLUMN;
        }
    )
    {
        return KronotopSqlDdlNodes.alterTable(s.end(this), name, alterType, newTableName, columnName, newColumnName, columnList);
    }
}