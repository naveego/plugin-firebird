using System.Collections.Generic;

namespace PluginFirebird.DataContracts
{
    public class ReplicationTable
    {
        public string TableName { get; set; }
        public List<ReplicationColumn> Columns { get; set; }
    }
}