using System.Data;
using PluginFirebird.Helper;

namespace PluginFirebird.API.Factory
{
    public interface IConnectionFactory
    {
        void Initialize(Settings settings);
        IConnection GetConnection();
        IConnection GetConnection(string database);
        ICommand GetCommand(string commandText, IConnection conn);
    }
}