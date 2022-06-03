using Naveego.Sdk.Plugins;
using PluginFirebird.API.Utility;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetGoldenReplicationTable(Schema schema, string safeSchemaName, string safeGoldenTableName)
        {
            var goldenTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeGoldenTableName);
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                DataType = "VARCHAR(255)",
                PrimaryKey = true
            });
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionIds,
                // In FirebirdDB, Long text is the Blob > Text subtype
                DataType = "BLOB SUB_TYPE 1",
                PrimaryKey = false,
                Serialize = true
            });

            return goldenTable;
        }
    }
}