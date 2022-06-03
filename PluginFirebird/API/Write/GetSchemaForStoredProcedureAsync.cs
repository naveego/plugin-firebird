using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginFirebird.API.Factory;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Write
{
    public static partial class Write
    {
        private static string ParamName = "PARAMETER_NAME";
        private static string DataType = "DATA_TYPE";

//         private static string GetStoredProcedureParamsQuery = @"
// SELECT PARAMETER_NAME, DATA_TYPE, ORDINAL_POSITION
// FROM INFORMATION_SCHEMA.PARAMETERS
// WHERE SPECIFIC_SCHEMA = '{0}'
// AND SPECIFIC_NAME = '{1}'
// ORDER BY ORDINAL_POSITION ASC";

        private static string QueryGetStoredProcedureParams = @"
SELECT p.RDB$PROCEDURE_NAME AS PROCEDURE_NAME
    , p.RDB$PARAMETER_NAME AS PARAMETER_NAME
    , p.RDB$PARAMETER_NUMBER AS PARAMETER_NUMBER
    , f.RDB$FIELD_TYPE AS DATA_TYPE

FROM RDB$PROCEDURE_PARAMETERS AS p
    LEFT JOIN RDB$RELATION_FIELDS AS rf ON rf.RDB$RELATION_NAME = p.RDB$RELATION_NAME AND p.RDB$FIELD_NAME = rf.RDB$RELATION_NAME
    LEFT JOIN RDB$FIELDS AS f ON p.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME

WHERE p.RDB$PROCEDURE_NAME = '{0}'

ORDER BY p.RDB$PARAMETER_NUMBER ASC";

        /// <summary>
        /// Creates a schema based on a specific stored procedure
        /// </summary>
        /// <param name="connFactory">The connection factory for the FBConnection</param>
        /// <param name="storedProcedure">Stored procedure for which a schema will be created</param>
        /// <returns>A Task that produces the schema for the specified procedure</returns>
        public static async Task<Schema> GetSchemaForStoredProcedureAsync(IConnectionFactory connFactory,
            WriteStoredProcedure storedProcedure)
        {
            // create a new schema object
            var schema = new Schema
            {
                Id = storedProcedure.GetName(),
                Name = storedProcedure.GetName(),
                Description = "",
                DataFlowDirection = Schema.Types.DataFlowDirection.Write,
                Query = storedProcedure.GetName()
            };

            // establish a connection
            var conn = connFactory.GetConnection();
            await conn.OpenAsync();

            // query the parameters of the stored procedure
            var cmd = connFactory.GetCommand(
                // --- Note: Multiple schemas not supported in FirebirdDB
                string.Format(QueryGetStoredProcedureParams, /*storedProcedure.SchemaName,*/ storedProcedure.ProcedureName),
                conn);
            var reader = await cmd.ExecuteReaderAsync();

            // for each parameter,
            while (await reader.ReadAsync())
            {
                // create a new property...
                var property = new Property
                {
                    // (remember to trim input from the reader to avoid trailing whitespace)
                    Id = reader.GetValueById(ParamName).ToString()?.Trim(),
                    Name = reader.GetValueById(ParamName).ToString()?.Trim(),
                    Description = "",
                    Type = Discover.Discover.GetType(reader.GetValueById(DataType).ToString()?.Trim()),
                    TypeAtSource = reader.GetValueById(DataType).ToString()?.Trim()
                };

                // ...and add it to the schema
                schema.Properties.Add(property);
            }

            // close the connection
            await conn.CloseAsync();

            // export the resulting schema
            return schema;
        }
    }
}