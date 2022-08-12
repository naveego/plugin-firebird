using System;
using System.Collections.Generic;
using System.Linq;
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
        private const string TableSchema = "TABLE_SCHEMA";
        private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "DATA_TYPE";
        private const string ColumnKey = "COLUMN_KEY";
        private const string IsNullable = "IS_NULLABLE";
        private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";
        
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

        private const string QueryAllTablesAndColumns = @"
select distinct r.rdb$relation_name as TABLE_NAME
    , 'FBDBSchema1' as TABLE_SCHEMA
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
    , CASE rc.RDB$CONSTRAINT_TYPE
        WHEN 'PRIMARY KEY' THEN 'YES'
        ELSE 'NO'
      END as COLUMN_KEY
    , CASE rf.RDB$NULL_FLAG
        WHEN 1 THEN 'NO'
        ELSE 'YES'
      END AS IS_NULLABLE
    , rc.RDB$CONSTRAINT_NAME as CONSTRAINT_NAME
    , rc.RDB$CONSTRAINT_TYPE as CONSTRAINT_TYPE
    , sg.RDB$INDEX_NAME as INDEX_NAME
    , rf.RDB$FIELD_SOURCE as FIELD_SOURCE
    , ix.RDB$RELATION_NAME as INDEX_RELATION
    , f.RDB$CHARACTER_LENGTH AS CHARACTER_MAXIMUM_LENGTH

from
    RDB$RELATIONS AS r
    left join RDB$RELATION_FIELDS AS rf ON r.RDB$RELATION_NAME = rf.RDB$RELATION_NAME
    left join RDB$FIELDS AS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
    left join rdb$index_segments AS sg on sg.RDB$FIELD_NAME = rf.RDB$FIELD_NAME
    left join rdb$relation_constraints AS rc on rc.RDB$RELATION_NAME = r.RDB$RELATION_NAME
        AND rc.RDB$INDEX_NAME = sg.RDB$INDEX_NAME
    left join rdb$indices AS ix on (
        ix.RDB$RELATION_NAME = r.RDB$RELATION_NAME
        AND rc.RDB$RELATION_NAME = r.RDB$RELATION_NAME
    )

where
    SUBSTRING (r.RDB$RELATION_NAME FROM 4 FOR 1) <> '$' -- Ignores 'magical tables' (RDB, MON, and SEC)
        AND (sg.RDB$INDEX_NAME IS NULL OR SUBSTRING (sg.RDB$INDEX_NAME FROM 4 FOR 1) <> '$')
        AND (rc.RDB$CONSTRAINT_TYPE IS NULL OR rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY')
        -- Removes duplicate segments that AREN'T part of constraints of the current table
        AND NOT (rc.RDB$CONSTRAINT_NAME IS NULL AND sg.RDB$INDEX_NAME IS NOT NULL)


order by TABLE_NAME, rf.RDB$FIELD_ID ASC";

        public static async IAsyncEnumerable<Schema> GetAllSchemas(IConnectionFactory connFactory, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                var cmd1 = connFactory.GetCommand(QueryAllTablesAndColumns, conn);
                var reader = await cmd1.ExecuteReaderAsync();

                Schema schema = null;
                var currentSchemaId = "";
                while (await reader.ReadAsync())
                {
                    var schemaId = $"{Utility.Utility.GetSafeName(reader.GetValueById(TableName).ToString()?.Trim(), '"')}";
                    
                    if (schemaId != currentSchemaId)
                    {
                        // return previous schema
                        if (schema != null)
                        {
                            yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                        }

                        // start new schema
                        currentSchemaId = schemaId;
                        var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                        schema = new Schema
                        {
                            Id = currentSchemaId,
                            Name = $"{parts.Table.Trim()}",
                            DataFlowDirection = Schema.Types.DataFlowDirection.Read
                        };
                    }

                    // add column to schema
                    var property = new Property
                    {
                        Id = Utility.Utility.GetSafeName(reader.GetValueById(ColumnName).ToString()?.Trim()),
                        Name = reader.GetValueById(ColumnName).ToString()?.Trim(),
                        IsKey = reader.GetValueById(ColumnKey).ToString() == "YES",
                        IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(reader.GetValueById(DataType).ToString()?.Trim()),
                        TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString()?.Trim(),
                            reader.GetValueById(CharacterMaxLength))
                    };
                    schema?.Properties.Add(property);
                }

                if (schema != null)
                {
                    // get sample and count
                    yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                }
            }
            finally
            {
                await conn.CloseAsync();
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

        public static PropertyType GetType(string dataType)
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
                    return PropertyType.String;
                case "TEXT":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}