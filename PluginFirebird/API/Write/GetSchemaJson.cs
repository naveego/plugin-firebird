using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Write
{
    public static partial class Write
    {
        public static string GetSchemaJson(List<WriteStoredProcedure> storedProcedures)
        {
            var schemaJsonObj = new Dictionary<string, object>
            {
                {"type", "object"},
                {"properties", new Dictionary<string, object>
                {
                    {"StoredProcedure", new Dictionary<string, object>
                    {
                        {"type", "string"},
                        {"title", "Stored Procedure"},
                        {"description", "Stored Procedure to call"},
                        {"enum", storedProcedures.Select(s => s.GetName())}
                    }},
                }},
                {"required", new []
                {
                    "StoredProcedure"
                }}
            };

            return JsonConvert.SerializeObject(schemaJsonObj);
        }
    }
}