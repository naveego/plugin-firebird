using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginFirebird.API.Factory;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication
    {
        public static async Task DropTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            if (!await TableExistsAsync(connFactory, table)) return; // table doesn't exist
            
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(
                    $"DROP TABLE {Utility.Utility.GetSafeName(table.TableName, '"')}",
                    conn);
                await cmd.ExecuteNonQueryAsync();
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}