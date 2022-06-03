using PluginFirebird.API.Utility;

namespace PluginFirebird.DataContracts
{
    public class WriteStoredProcedure
    {
        //public string SchemaName { get; set; }
        public string ProcedureName { get; set; }
        public string ProcedureId { get; set; }

        public string GetName()
        {
            //return $"{Utility.GetSafeName(SchemaName)}.{Utility.GetSafeName(RoutineName)}";
            return Utility.GetSafeName(ProcedureName).Trim();
        }
    }
}