using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;

namespace PluginFirebird.API.Factory
{
    public class Command : ICommand
    {
        private readonly FbCommand _cmd;

        public Command()
        {
            _cmd = new FbCommand();
        }

        public Command(string commandText)
        {
            _cmd = new FbCommand(commandText);
        }

        public Command(string commandText, IConnection conn)
        {
            _cmd = new FbCommand(commandText, (FbConnection) conn.GetConnection());
        }

        public void SetConnection(IConnection conn)
        {
            _cmd.Connection = (FbConnection) conn.GetConnection();
        }

        public void SetCommandText(string commandText)
        {
            _cmd.CommandText = commandText;
        }

        public void AddParameter(string name, object value)
        {
            _cmd.Parameters.AddWithValue(name, value);
        }

        public async Task<IReader> ExecuteReaderAsync()
        {
            return new Reader(await _cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteNonQueryAsync()
        {
            return await _cmd.ExecuteNonQueryAsync();
        }
    }
}