using System;
using System.Collections.Generic;

namespace PluginFirebird.Helper
{
    public class Settings
    {
        public string Hostname { get; set; }
        public string Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(Hostname))
            {
                throw new Exception("The Hostname property must be set");
            }
            
            if (String.IsNullOrEmpty(Database))
            {
                throw new Exception("The Database property must be set");
            }

            if (String.IsNullOrEmpty(Username))
            {
                throw new Exception("The Username property must be set");
            }
            
            if (String.IsNullOrEmpty(Password))
            {
                throw new Exception("The Password property must be set");
            }
        }

        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return $"User={Username};Password={Password};Database={Database};Host={Hostname};Port={Port};";
        }
        
        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString(string database)
        {
            return $"User={Username};Password={Password};Database={database};Host={Hostname};Port={Port};";
        }
    }
}