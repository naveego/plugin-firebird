using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginFirebird.API.Factory;

namespace PluginFirebird.API.Discover
{
    public static partial class Discover
    {
//         private const string QueryTableAndColumns = @"
// SELECT t.TABLE_NAME
//      , t.TABLE_SCHEMA
//      , t.TABLE_TYPE
//      , c.COLUMN_NAME
//      , c.DATA_TYPE
//      , c.COLUMN_KEY
//      , c.IS_NULLABLE
//      , c.CHARACTER_MAXIMUM_LENGTH
//
// FROM INFORMATION_SCHEMA.TABLES AS t
//       INNER JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
//
// WHERE t.TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
// AND t.TABLE_SCHEMA = '{0}'
// AND t.TABLE_NAME = '{1}' 
//
// ORDER BY t.TABLE_NAME";

        private const string QueryTableAndColumns = @"
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
        AND r.RDB$RELATION_NAME = '{0}'


order by TABLE_NAME, rf.RDB$FIELD_ID ASC";

        public static async Task<Schema> GetRefreshSchemaForTable(IConnectionFactory connFactory, Schema schema,
            int sampleSize = 5)
        {
            var decomposed = DecomposeSafeName(schema.Id).TrimEscape();
            var conn = string.IsNullOrWhiteSpace(decomposed.Database)
                ? connFactory.GetConnection()
                : connFactory.GetConnection(decomposed.Database);

            try
            {
                await conn.OpenAsync();

                // --- Note: ---
                // FirebirdDB does not support multi-schema databases,
                // rather several smaller databases acting as isolated, disconnected schemas
                
                // var cmd = connFactory.GetCommand(
                //     string.Format(QueryTableAndColumns, decomposed.Schema, decomposed.Table), conn);
                var cmd = connFactory.GetCommand(string.Format(
                    QueryTableAndColumns,
                    decomposed.Table
                ), conn);
                var reader = await cmd.ExecuteReaderAsync();
                var refreshProperties = new List<Property>();

                while (await reader.ReadAsync())
                {
                    // add column to refreshProperties
                    var property = new Property
                    {
                        Id = Utility.Utility.GetSafeName(reader.GetValueById(ColumnName).ToString()?.Trim(), '"'),
                        Name = reader.GetValueById(ColumnName).ToString()?.Trim(),
                        IsKey = reader.GetValueById(ColumnKey).ToString() == "PRI",
                        IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(reader.GetValueById(DataType).ToString()?.Trim()),
                        TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString()?.Trim(),
                            reader.GetValueById(CharacterMaxLength))
                    };
                    refreshProperties.Add(property);
                }

                // add properties
                schema.Properties.Clear();
                schema.Properties.AddRange(refreshProperties);

            

                // get sample and count
                return await AddSampleAndCount(connFactory, schema, sampleSize);
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        private static DecomposeResponse DecomposeSafeName(string schemaId)
        {
            var response = new DecomposeResponse
            {
                Database = "",
                Schema = "",
                Table = ""
            };
            var parts = schemaId.Split('.');

            switch (parts.Length)
            {
                case 0:
                    return response;
                case 1:
                    response.Table = parts[0];
                    return response;
                case 2:
                    response.Schema = parts[0];
                    response.Table = parts[1];
                    return response;
                case 3:
                    response.Database = parts[0];
                    response.Schema = parts[1];
                    response.Table = parts[2];
                    return response;
                default:
                    return response;
            }
        }

        private static DecomposeResponse TrimEscape(this DecomposeResponse response, char escape = '"')
        {
            response.Database = response.Database.Trim(escape);
            response.Schema = response.Schema.Trim(escape);
            response.Table = response.Table.Trim(escape);

            return response;
        }
    }

    class DecomposeResponse
    {
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Table { get; set; }
    }
}