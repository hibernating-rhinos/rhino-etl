namespace Rhino.Etl.Core.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Default <see cref="IConnectionProvider"/> implementation which depends on
    /// application configuration file for connection string settings.
    /// </summary>
    public class ConnectionProvider : IConnectionProvider
    {
        private static IConnectionProvider _default;

        /// <summary>
        /// Gets or sets default <see cref="IConnectionProvider"/> instance.
        /// </summary>
        public static IConnectionProvider Default
        {
            get { return _default ?? (_default = new ConnectionProvider()); }
            set { _default = value; }
        }

        /// <inheritdoc />
        public ConnectionStringCollection ConnectionStrings { get; } = new ConnectionStringCollection();

        /// <inheritdoc />
        public virtual IDbConnection OpenConnection(string name)
        {
            var connectionString = ConnectionStrings[name];
            if (connectionString == null)
                throw new KeyNotFoundException("Could not find connnection string: " + name);

            return OpenConnection(connectionString);
        }

        /// <inheritdoc />
        public virtual IDbConnection OpenConnection(ConnectionStringSettings connectionString)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));
            if (connectionString.ProviderName == null)
                throw new ArgumentException("Null ProviderName specified", nameof(connectionString));

            IDbConnection connection = null;

            // Backwards compatibility: ProviderName could be an assembly qualified connection type name.
            Type connectionType = Type.GetType(connectionString.ProviderName);
            if (connectionType != null)
            {
                connection = Activator.CreateInstance(connectionType) as IDbConnection;
            }

            if (connection == null)
            {
                // ADO.NET compatible usage of provider name.
#if FEATURE_WESTWIND
                connection = Westwind.Utilities.DataUtils.GetDbProviderFactory(connectionString.ProviderName.ToLowerInvariant()).CreateConnection();
#else
                connection = DbProviderFactories.GetFactory(connectionString.ProviderName).CreateConnection();
#endif
            }

            connection.ConnectionString = connectionString.ConnectionString;
            connection.Open();
            return connection;
        }
    }
}