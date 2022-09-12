using System.Collections.Generic;
using Naveego.Sdk.Plugins;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication 
    {
        public static ReplicationTable ConvertSchemaToReplicationTable(Schema schema,
            string tableName)
        {
            var table = new ReplicationTable
            {
                TableName = tableName,
                Columns = new List<ReplicationColumn>()
            };
            
            foreach (var property in schema.Properties)
            {
                var column = new ReplicationColumn
                {
                    ColumnName = property.Name,
                    DataType = string.IsNullOrWhiteSpace(property.TypeAtSource)? GetType(property.Type): property.TypeAtSource,
                    PrimaryKey = false
                };
                
                table.Columns.Add(column);
            }

            return table;
        }
        
        private static string GetType(PropertyType dataType)
        {
            switch (dataType)
            {
                case PropertyType.Datetime:
                    return "TIMESTAMP";
                case PropertyType.Date:
                    return "DATE";
                case PropertyType.Time:
                    return "TIME";
                case PropertyType.Integer:
                    return "INTEGER";
                case PropertyType.Decimal:
                    return "DECIMAL(38,18)";
                case PropertyType.Float:
                    return "DOUBLE PRECISION";
                case PropertyType.Bool:
                    return "BOOLEAN";
                case PropertyType.Blob:
                    return "BLOB";
                case PropertyType.String:
                    return "VARCHAR(255)";
                case PropertyType.Text:
                    // In Firebird, Long text is a Blob > Text subtype
                    return "BLOB SUB_TYPE 1";
                default:
                    return "BLOB SUB_TYPE 1";
            }
        }
    }
}