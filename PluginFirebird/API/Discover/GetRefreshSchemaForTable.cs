using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginFirebird.API.Factory;

namespace PluginFirebird.API.Discover
{
    public static partial class Discover
    {
        public static async Task<Schema> GetRefreshSchemaForTable(IConnectionFactory connFactory, Schema schema,
            int sampleSize = 5)
        {
            var decomposed = DecomposeSafeName(schema.Id).TrimEscape();
            var refreshProperties = new List<Property>();
            var conn = string.IsNullOrWhiteSpace(decomposed.Database)
                ? connFactory.GetConnection()
                : connFactory.GetConnection(decomposed.Database);

            // Pass 1: Get all columns for table
            try
            {
                await conn.OpenAsync();

                // --- Note: ---
                // FirebirdDB does not support multi-schema databases,
                // rather several smaller databases acting as isolated, disconnected schemas
                
                var cmd = connFactory.GetCommand(GetDiscoverQuery(false, decomposed.Table), conn);
                var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    // add column to refreshProperties
                    var property = new Property
                    {
                        Id = Utility.Utility.GetSafeName(reader.GetValueById(ColumnName).ToString()?.Trim(), '"'),
                        Name = reader.GetValueById(ColumnName).ToString()?.Trim(),
                        IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(reader.GetValueById(DataType).ToString()?.Trim()),
                        TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString()?.Trim(),
                            reader.GetValueById(CharacterMaxLength))
                    };
                    refreshProperties.Add(property);
                }
                
                // Pass 2: Find PK columns and mark them as PK 
                var pkCmd = connFactory.GetCommand(GetDiscoverQuery(true, decomposed.Table), conn);
                var pkReader = await pkCmd.ExecuteReaderAsync();
                
                while (await pkReader.ReadAsync())
                {
                    var pkColName = pkReader.GetValueById(ColumnKey).ToString()?.Trim();
                    var pkCol = refreshProperties.FirstOrDefault(p => p.Name == pkColName);

                    if (pkCol != null)
                    {
                        pkCol.IsKey = true;
                    }
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
        
        public static async Task<Schema> FindSchemaForTable(IConnectionFactory connFactory, Schema schema)
        {
            var decomposed = DecomposeSafeName(schema.Id).TrimEscape();
            var refreshProperties = new List<Property>();
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(GetDiscoverQuery(false, decomposed.Table), conn);
                var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    // add column to refreshProperties
                    var property = new Property
                    {
                        Id = Utility.Utility.GetSafeName(reader.GetValueById(ColumnName).ToString()),
                        Name = reader.GetValueById(ColumnName).ToString()?.Trim(),
                        IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(
                            reader.GetValueById(DataType).ToString(),
                            reader.GetValueById(CharacterMaxLength)),
                        TypeAtSource = GetTypeAtSource(
                            reader.GetValueById(DataType).ToString(),
                            reader.GetValueById(CharacterMaxLength),
                            reader.GetValueById(DataPrecision),
                            reader.GetValueById(DataScale))
                    };

                    var prevProp = refreshProperties.FirstOrDefault(p => p.Id == property.Id);
                    if (prevProp == null)
                    {
                        refreshProperties.Add(property);
                    }
                    else
                    {
                        var index = refreshProperties.IndexOf(prevProp);
                        refreshProperties.RemoveAt(index);

                        property.IsKey = prevProp.IsKey || property.IsKey;
                        refreshProperties.Add(property);
                    }
                }
                
                // Find PK columns and mark them as PK
                var pkCmd = connFactory.GetCommand(GetDiscoverQuery(true, decomposed.Table), conn);
                var pkReader = await pkCmd.ExecuteReaderAsync();
                
                while (await pkReader.ReadAsync())
                {
                    var pkColName = pkReader.GetValueById(ColumnKey).ToString()?.Trim();
                    var pkCol = refreshProperties.FirstOrDefault(p => p.Name == pkColName);

                    if (pkCol != null)
                    {
                        pkCol.IsKey = true;
                    }
                }

                // add properties
                schema.Properties.Clear();
                schema.Properties.AddRange(refreshProperties);
                
                return schema;
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
                    response.Database = parts[0];
                    response.Table = parts[1];
                    return response;
                default:
                    return response;
            }
        }

        private static DecomposeResponse TrimEscape(this DecomposeResponse response, char escape = '"')
        {
            response.Database = response.Database.Trim(escape);
            response.Table = response.Table.Trim(escape);

            return response;
        }
    }

    class DecomposeResponse
    {
        public string Database { get; set; }
        public string Table { get; set; }
    }
}