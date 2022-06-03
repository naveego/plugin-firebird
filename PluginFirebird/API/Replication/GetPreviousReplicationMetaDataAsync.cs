using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginFirebird.API.Factory;
using PluginFirebird.API.Utility;
using PluginFirebird.DataContracts;
using PluginFirebird.Helper;
using Constants = PluginFirebird.API.Utility.Constants;

namespace PluginFirebird.API.Replication
{
    public static partial class Replication
    {
        private static readonly string QueryCountMetaData = @"SELECT COUNT(*) FROM {0} WHERE {1} = '{2}'";
        private static readonly string QueryGetMetaData = @"SELECT * FROM {0} WHERE {1} = '{2}'";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(
            IConnectionFactory connFactory,
            string jobId,
            ReplicationTable table)
        {
            var conn = connFactory.GetConnection();

            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureTableAsync(connFactory, table);

                // check if metadata exists

                await conn.OpenAsync();

                // use count query to determine # of rows
                var countCmd = connFactory.GetCommand(
                    string.Format(QueryCountMetaData,
                        //Utility.Utility.GetSafeName(table.SchemaName, '"'),
                        Utility.Utility.GetSafeName(table.TableName, '"'),
                        Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
                        jobId),
                    conn);
                var countReader = await countCmd.ExecuteReaderAsync();
                await countReader.ReadAsync();

                var metaDataCount = (long)countReader.GetValueById("\"COUNT\"");
                if (metaDataCount > 0) // metadata exists
                {
                    var cmd = connFactory.GetCommand(
                        string.Format(QueryGetMetaData,
                            //Utility.Utility.GetSafeName(table.SchemaName, '"'),
                            Utility.Utility.GetSafeName(table.TableName, '"'),
                            Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
                            jobId),
                        conn);
                    var reader = await cmd.ExecuteReaderAsync();

                    // if (reader.HasRows())
                    // {
                    await reader.ReadAsync();

                    var request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
                        reader.GetValueById(Constants.ReplicationMetaDataRequest).ToString());
                    var shapeName = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeName)
                        .ToString();
                    var shapeId = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeId)
                        .ToString();
                    var timestamp = DateTime.Parse(reader.GetValueById(Constants.ReplicationMetaDataTimestamp)
                        .ToString());

                    replicationMetaData = new ReplicationMetaData
                    {
                        Request = request,
                        ReplicatedShapeName = shapeName,
                        ReplicatedShapeId = shapeId,
                        Timestamp = timestamp
                    };
                    //}
                }

                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}