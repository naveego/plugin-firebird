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
                {"description", "Firebird databases only contain one schema. The golden record and version tables will be stored in this schema."},
                {"properties", new Dictionary<string, object>
                {
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
                    "GoldenTableName",
                    "VersionTableName"
                }}
            };
            
            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}