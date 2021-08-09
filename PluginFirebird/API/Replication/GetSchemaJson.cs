using System.Collections.Generic;
using Newtonsoft.Json;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication
    {
        public static string GetSchemaJson()
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"SchemaName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Schema Name"},
                        {"description", "Name of schema to put golden and version tables into in Firebird"},
                    }},
                    {"GoldenTableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Golden Record Table Name"},
                        {"description", "Name for your golden record table in Firebird"},
                    }},
                    {"VersionTableName", new Dictionary<string, string>
                    {
                        {"type", "string"},
                        {"title", "Version Record Table Name"},
                        {"description", "Name for your version record table in Firebird"},
                    }},
                }},
                {"required", new []
                {
                    "SchemaName",
                    "GoldenTableName",
                    "VersionTableName"
                }}
            };
            
            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}