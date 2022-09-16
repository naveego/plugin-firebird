using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using PluginFirebird.API.Factory;
using PluginFirebird.DataContracts;
using PluginFirebird.Helper;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication
    { 
        // Source: https://stackoverflow.com/questions/18114458/fastest-way-to-determine-if-record-exists

        private static readonly string EnsureTableQuery = @"
SELECT COUNT(*) as c
FROM RDB$RELATIONS
WHERE RDB$RELATION_NAME = '{0}'
";

        public static async Task<bool> TableExistsAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();
                
                Logger.Info(
                    $"Checking for Table: {string.Format(EnsureTableQuery, table.TableName)}");
                var cmd = connFactory.GetCommand(
                    string.Format(EnsureTableQuery, table.TableName), conn);
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var count = (long)reader.GetValueById("c");

                return count > 0;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        public static async Task EnsureTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            if (await TableExistsAsync(connFactory, table)) return; // table already exists
            
            var conn = connFactory.GetConnection();

            try
            {
                // create table statement
                var querySb =
                    new StringBuilder($@"CREATE TABLE {Utility.Utility.GetSafeName(table.TableName, '"')} (");
                querySb.Append("\n");

                // nested primary key constraint statement
                var primaryKeySb = new StringBuilder($@"CONSTRAINT {Utility.Utility.GetSafeName(table.TableName)}");
                primaryKeySb.Length--;
                primaryKeySb.Append("_PK\" PRIMARY KEY (");
                var hasPrimaryKey = false;

                foreach (var column in table.Columns)
                {
                    querySb.Append(
                        $"{Utility.Utility.GetSafeName(column.ColumnName)} {column.DataType}{(column.PrimaryKey ? " NOT NULL" : "")},\n"
                    );

                    // skip if not primary key
                    if (!column.PrimaryKey) continue;

                    // add primary key as a constraint
                    primaryKeySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                    hasPrimaryKey = true;
                }

                if (hasPrimaryKey)
                {
                    primaryKeySb.Length--;
                    primaryKeySb.Append(")");
                    querySb.Append($"{primaryKeySb});");
                }
                else
                {
                    querySb.Length--;
                    querySb.Append(");");
                }

                var query = querySb.ToString();
                Logger.Info($"Creating Table: {query}");

                await conn.OpenAsync();
                var cmd = connFactory.GetCommand(query, conn);

                await cmd.ExecuteNonQueryAsync();
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}