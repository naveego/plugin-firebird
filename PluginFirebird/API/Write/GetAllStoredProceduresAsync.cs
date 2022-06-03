using System.Collections.Generic;
using System.Threading.Tasks;
using PluginFirebird.API.Factory;
using PluginFirebird.DataContracts;

namespace PluginFirebird.API.Write
{
    public static partial class Write
    {
        // --- Note: ---
        // Only single-schema databases inside of Firebird DB files
        
        //private const string SchemaName = "ROUTINE_SCHEMA";
        private const string ProcedureName = "RDB$PROCEDURE_NAME";
        private const string ProcedureId = "RDB$PROCEDURE_ID";

        private static string GetAllStoredProceduresQuery = @"
SELECT RDB$PROCEDURE_NAME, RDB$PROCEDURE_ID
FROM RDB$PROCEDURES AS p
WHERE SUBSTRING (p.RDB$PROCEDURE_NAME FROM 4 FOR 1) <> '$'";

        public static async Task<List<WriteStoredProcedure>> GetAllStoredProceduresAsync(IConnectionFactory connFactory)
        {
            // list for tracking stored procedures
            var storedProcedures = new List<WriteStoredProcedure>();
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                // get all stored procedures from the database
                var cmd = connFactory.GetCommand(GetAllStoredProceduresQuery, conn);
                var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    // for each procedure, add it to the list
                    var storedProcedure = new WriteStoredProcedure
                    {
                        //SchemaName = reader.GetValueById(SchemaName).ToString(),
                        ProcedureName = reader.GetValueById(ProcedureName).ToString()?.Trim(),
                        ProcedureId = reader.GetValueById(ProcedureId).ToString()?.Trim()
                    };

                    storedProcedures.Add(storedProcedure);
                }

                // export stored procedures
                return storedProcedures;
            }
            finally
            {
                // free unused, resource-heavy services
                // close the connection
                await conn.CloseAsync();
            }
        }
    }
}