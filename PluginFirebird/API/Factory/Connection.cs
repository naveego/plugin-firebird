using System.Data;
using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;
using PluginFirebird.Helper;

namespace PluginFirebird.API.Factory
{
    public class Connection : IConnection
    {
        private readonly FbConnection _conn;

        public Connection(Settings settings)
        {
            _conn = new FbConnection(settings.GetConnectionString());
        }

        public Connection(Settings settings, string database)
        {
            _conn = new FbConnection(settings.GetConnectionString(database));
        }

        public async Task OpenAsync()
        {
            await _conn.OpenAsync();
        }

        public async Task CloseAsync()
        {
            await _conn.CloseAsync();
        }

        public async Task<bool> PingAsync()
        {
            // return await _conn.PingAsync();
            return true;
        }

        public IDbConnection GetConnection()
        {
            return _conn;
        }
    }
}