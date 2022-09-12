using System.Threading.Tasks;
using PluginFirebird.API.Factory;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) as c
FROM (
SELECT * FROM {0}
WHERE {1} = '{2}'    
) as q";

        public static async Task<bool> RecordExistsAsync(IConnectionFactory connFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();
            
                var cmd = connFactory.GetCommand(string.Format(RecordExistsQuery,
                        // --- Note: Firebird DBs only support single-schema databases (multiple tables)
                        //Utility.Utility.GetSafeName(table.SchemaName, '"'),
                        Utility.Utility.GetSafeName(table.TableName, '"'),
                        Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey).ColumnName, '"'),
                        primaryKeyValue
                    ),
                    conn);

                // check if record exists
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var count = (long) reader.GetValueById("c");
                
                return count != 0;
            }
            finally
            {
                await conn.CloseAsync();
            }
            
        }
    }
}