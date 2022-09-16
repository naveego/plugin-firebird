using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginFirebird.API.Factory;
using FirebirdSql.Data;
using PluginFirebird.Helper;

// Source: Firebird 4.0 Language Reference
//      https://firebirdsql.org/file/documentation/html/en/refdocs/fblangref40/firebird-40-language-reference.html

namespace PluginFirebird.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "TABLE_NAME";
        //private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "DATA_TYPE";
        private const string ColumnKey = "PK_FIELD_NAME";
        private const string IsNullable = "IS_NULLABLE";
        private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";
        private const string DataPrecision = "DATA_PRECISION";
        private const string DataScale = "DATA_SCALE";
        
        // ----- All Tables And Columns Query -----
        // Source: https://stackoverflow.com/questions/10945384/firebird-sql-statement-to-get-the-table-definition
        // Source: https://stackoverflow.com/questions/36617891/find-all-column-names-that-are-primary-keys-in-firebird-database
        // Source: https://firebirdsql.org/file/documentation/html/en/refdocs/fblangref40/firebird-40-language-reference.html#fblangref40-datatypes
        
        // --- Notes: ---
        // Table RDB$FIELDS.RDB$FIELD_NAME links to RDB$RELATION_FIELDS.RDB$FIELD_SOURCE,
        // NOT: RDB$RELATION_FIELDS.RDB$FIELD_NAME
        
        // * - Firebird DBs only have 1 schema
        // Can have multiple DBs for many smaller schemas
        // TABLE_SCHEMA is the table's OWNER_NAME field

        private const string QueryAllTablesAndColumns = @"select distinct r.rdb$relation_name as TABLE_NAME
    , r.RDB$RELATION_TYPE as TABLE_TYPE
    , rf.RDB$FIELD_NAME as COLUMN_NAME
    , CASE
        -- If field type is an Integer and field sub type is greater than 0...
        WHEN f.RDB$FIELD_TYPE IN ( 7, 8, 16, 26 ) AND f.RDB$FIELD_SUB_TYPE > 0 THEN
            -- Column type is either NUMERIC or DECIMAL
            CASE f.RDB$FIELD_SUB_TYPE
                -- SUBTYPES of INTEGERs here:
                -- FirebirdSQL makes NUMERIC and DECIMAL types special subtypes of integers
                WHEN 1 THEN 'NUMERIC'
                WHEN 2 THEN 'DECIMAL'
            END
        -- If field type is a BLOB and field sub type is > 0...
        WHEN f.RDB$FIELD_TYPE IN ( 261 ) AND f.RDB$FIELD_SUB_TYPE > 0 THEN
            CASE f.RDB$FIELD_SUB_TYPE
                -- SUBTYPES of BLOG here:
                -- FirebirdSQL makes TEXT a subtype of BLOG
                WHEN 1 THEN 'TEXT'
                ELSE 'BLOB'
            END
        -- Other types here...
        ELSE CASE f.RDB$FIELD_TYPE
            WHEN 7 THEN 'SMALLINT'
            WHEN 8 THEN 'INTEGER'
            WHEN 10 THEN 'FLOAT'
            WHEN 12 THEN 'DATE'
            WHEN 13 THEN 'TIME'
            WHEN 14 then 'CHAR'
            WHEN 16 THEN 'BIGINT'
            WHEN 23 THEN 'BOOLEAN'
            WHEN 24 THEN 'DECFLOAT(16)'
            WHEN 25 THEN 'DECFLOAT(34)'
            WHEN 26 THEN 'INT128'
            WHEN 27 THEN 'DOUBLE PRECISION'
            WHEN 28 THEN 'TIME W/TIME ZONE'
            WHEN 29 THEN 'TIMESTAMP W/TIME ZONE'
            WHEN 35 THEN 'TIMESTAMP'
            WHEN 37 THEN 'VARCHAR'
            WHEN 261 THEN 'BLOB'
            ELSE 'OTHER'
          END
      END AS DATA_TYPE
    , CASE rf.RDB$NULL_FLAG
        WHEN 1 THEN 'NO'
        ELSE 'YES'
      END AS IS_NULLABLE
    , f.RDB$CHARACTER_LENGTH AS CHARACTER_MAXIMUM_LENGTH
    --, f.RDB$FIELD_PRECISION AS DATA_PRECISION
    , CASE
        -- If field type is a DECIMAL or NUMERIC type, return precision
        WHEN f.RDB$FIELD_TYPE IN ( 7, 8, 16, 26 ) AND f.RDB$FIELD_SUB_TYPE > 0
          THEN f.RDB$FIELD_PRECISION
        -- Other types: Null
        ELSE NULL
    END AS DATA_PRECISION
    , CASE
        -- If field type is a DECIMAL or NUMERIC type, return scale
        WHEN f.RDB$FIELD_TYPE IN ( 7, 8, 16, 26 ) AND f.RDB$FIELD_SUB_TYPE > 0
          THEN f.RDB$FIELD_SCALE
        -- Other types: Null
        ELSE NULL
    END AS DATA_SCALE
from
    RDB$RELATIONS AS r
    left join RDB$RELATION_FIELDS AS rf ON r.RDB$RELATION_NAME = rf.RDB$RELATION_NAME
    left join RDB$FIELDS AS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
where
    SUBSTRING (r.RDB$RELATION_NAME FROM 4 FOR 1) <> '$' -- Ignores system tables (RDB, MON, and SEC)
        {0}
order by TABLE_NAME, rf.RDB$FIELD_ID ASC";

        private const string QueryPrimaryKeyColumns = @"select distinct r.rdb$relation_name as TABLE_NAME
    , rf.RDB$FIELD_NAME as PK_FIELD_NAME
from
    RDB$RELATIONS AS r
    left join rdb$relation_constraints AS rc on r.RDB$RELATION_NAME = rc.RDB$RELATION_NAME
    left join rdb$indices AS ix on rc.RDB$INDEX_NAME = ix.RDB$INDEX_NAME
    left join rdb$index_segments AS sg on sg.RDB$INDEX_NAME = ix.RDB$INDEX_NAME
    left join RDB$RELATION_FIELDS AS rf ON sg.RDB$FIELD_NAME = rf.RDB$FIELD_NAME
where
    SUBSTRING (r.RDB$RELATION_NAME FROM 4 FOR 1) <> '$' -- Ignores system tables (RDB, MON, and SEC)
        AND (sg.RDB$INDEX_NAME IS NULL OR SUBSTRING (sg.RDB$INDEX_NAME FROM 4 FOR 1) <> '$')
        AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
            {0}
order by TABLE_NAME, rf.RDB$FIELD_ID ASC";
        
        private const string QueryWhereTableClause = @"AND r.RDB$RELATION_NAME = '{0}'";

        public static string GetDiscoverQuery(bool primaryKeys, string tableName = null)
        {
            var whereClause = "";
            if (!string.IsNullOrWhiteSpace(tableName))
            {
                whereClause = string.Format(QueryWhereTableClause, tableName);
            }

            if (primaryKeys)
            {
                return string.Format(QueryPrimaryKeyColumns, whereClause);
            }

            return string.Format(QueryAllTablesAndColumns, whereClause);
        }
        
        public static async IAsyncEnumerable<Schema> GetAllSchemas(IConnectionFactory connFactory, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();
            var finalSchemas = new List<Schema>();

            // Pass 1: Get All Columns
            try
            {
                await conn.OpenAsync();
                
                var cmd1 = connFactory.GetCommand(GetDiscoverQuery(false), conn);
                var reader1 = await cmd1.ExecuteReaderAsync();

                Schema currentSchema = null;
                var currentSchemaId = "";
                while (await reader1.ReadAsync())
                {
                    var schemaId = $"{Utility.Utility.GetSafeName(reader1.GetValueById(TableName).ToString()?.Trim(), '"')}";
                    
                    if (schemaId != currentSchemaId)
                    {
                        // add previous schema to a list
                        if (currentSchema != null)
                        {
                            //yield return await AddSampleAndCount(connFactory, currentSchema, sampleSize);
                            finalSchemas.Add(currentSchema);
                        }

                        // start new schema
                        currentSchemaId = schemaId;
                        var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                        currentSchema = new Schema
                        {
                            Id = currentSchemaId,
                            Name = $"{parts.Table.Trim()}",
                            DataFlowDirection = Schema.Types.DataFlowDirection.Read,
                            Description = ""
                        };
                    }

                    // add column to schema
                    var property = new Property
                    {
                        Id = Utility.Utility.GetSafeName(reader1.GetValueById(ColumnName).ToString()?.Trim()),
                        Name = reader1.GetValueById(ColumnName).ToString()?.Trim(),
                        IsNullable = reader1.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(reader1.GetValueById(DataType).ToString()?.Trim(),
                            reader1.GetValueById(CharacterMaxLength)),
                        TypeAtSource = GetTypeAtSource(
                            reader1.GetValueById(DataType).ToString()?.Trim(),
                            reader1.GetValueById(CharacterMaxLength),
                            reader1.GetValueById(DataPrecision),
                            reader1.GetValueById(DataScale))
                    };
                    currentSchema?.Properties.Add(property);
                }
                
                if (currentSchema != null)
                {
                    // get sample and count
                    //yield return await AddSampleAndCount(connFactory, currentSchema, sampleSize);
                    finalSchemas.Add(currentSchema);
                }
                
                // Pass 2: Get All Primary Keys for each Table
                var cmd2 = connFactory.GetCommand(GetDiscoverQuery(true), conn);
                var reader2 = await cmd2.ExecuteReaderAsync();

                Schema currentPkSchema = null;
                var currentPkSchemaId = "";
                while (await reader2.ReadAsync())
                {
                    var schemaId =
                        $"{Utility.Utility.GetSafeName(reader2.GetValueById(TableName).ToString()?.Trim(), '"')}";

                    if (currentPkSchemaId != schemaId)
                    {
                        if (currentPkSchema != null)
                        {
                            // remove the schema from the final list
                            finalSchemas.RemoveAll(s => s.Id == currentPkSchemaId);

                            // get sample and count
                            yield return await AddSampleAndCount(connFactory, currentPkSchema, sampleSize);
                        }

                        // find matching schema in the list
                        currentPkSchemaId = schemaId;
                        currentPkSchema = finalSchemas.FirstOrDefault(s => s.Id == schemaId);
                    }

                    // Find the PK column name in the schema and switch on the PK flag
                    var pkColumnName =
                        $"{Utility.Utility.GetSafeName(reader2.GetValueById(ColumnKey).ToString()?.Trim(), '"')}"
                            .Trim('"');
                    var pkColumn = currentPkSchema.Properties.FirstOrDefault(p => p.Name == pkColumnName);
                    pkColumn.IsKey = true;
                }

                if (currentPkSchema != null)
                {
                    // remove the schema from the final list
                    finalSchemas.RemoveAll(s => s.Id == currentPkSchemaId);

                    // get sample and count
                    yield return await AddSampleAndCount(connFactory, currentPkSchema, sampleSize);
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
            
            // return any remaining schemas
            foreach (var s in finalSchemas)
            {
                yield return await AddSampleAndCount(connFactory, s, sampleSize);
            }
        }

        private static async Task<Schema> AddSampleAndCount(IConnectionFactory connFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(connFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(connFactory, schema);

            return schema;
        }

        public static PropertyType GetType(string dataType, object dataLength = null)
        {
            switch (dataType)
            {
                case "TIMESTAMP":
                case "TIMESTAMP W/TIME ZONE":
                    return PropertyType.Datetime;
                case "DATE":
                    return PropertyType.Date;
                case "TIME":
                case "TIME W/TIME ZONE":
                    return PropertyType.Time;
                case "SMALLINT":
                case "INTEGER":
                case "BIGINT":
                case "INT128":
                    return PropertyType.Integer;
                // DECFLOAT type is floating-point type (non-approximate)
                // NUMERIC type is like DECIMAL, but is stored "as declared"
                //      e.x. "3.14195" stored as NUMERIC(4, 2) becomes "3.14" (or 03.14)
                case "DECFLOAT(16)":
                case "DECFLOAT(34)":
                case "DECIMAL":
                case "NUMERIC":
                    return PropertyType.Decimal;
                case "FLOAT":
                case "DOUBLE PRECISION":
                    return PropertyType.Float;
                case "BOOLEAN":
                    return PropertyType.Bool;
                case "BLOB":
                    return PropertyType.Blob;
                case "CHAR":
                case "VARCHAR":
                    if (dataLength != null)
                    {
                        if (Int32.Parse($"{dataLength}") > 1024)
                        {
                            return PropertyType.Text;
                        }    
                    }
                    return PropertyType.String;
                case "TEXT":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object dataLength = null, object dataPrecision = null, object dataScale = null)
        {
            dataType = dataType.Trim();
            dataLength ??= DBNull.Value;
            dataPrecision ??= DBNull.Value;
            dataScale ??= DBNull.Value;

            var finalType = "";
            
            switch (dataType)
            {
                case "CHAR":
                case "VARCHAR":
                    if (dataLength != DBNull.Value)
                    {
                        finalType = $"{dataType}({dataLength})";
                    }
                    break;
                case "NUMERIC":
                case "DECIMAL":
                    if (dataPrecision != DBNull.Value)
                    {
                        var typeBuilder = new StringBuilder();
                        typeBuilder.Append($"{dataType}({dataPrecision}");

                        if (dataScale != DBNull.Value)
                        {
                            typeBuilder.Append($",{dataScale}");
                        }

                        typeBuilder.Append(")");
                        
                        finalType =  typeBuilder.ToString();
                    }
                    break;
                case "TEXT":
                    finalType = "BLOB SUB_TYPE 1";
                    break;
                case "TIME W/TIME ZONE":
                    finalType = "TIME WITH TIME ZONE";
                    break;
                case "TIMESTAMP W/TIME ZONE":
                    finalType = "TIMESTAMP WITH TIME ZONE";
                    break;
                case "OTHER":
                    finalType = "VARCHAR(255)";
                    break;
                default:
                    finalType = dataType;
                    break;
            }

            return finalType.Trim();
        }
    }
}