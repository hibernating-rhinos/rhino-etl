namespace Rhino.Etl.Core.Infrastructure
{
    using System.Configuration;
    using System.Data;

    /// <summary>
    /// Interface for database connection providers.
    /// </summary>
    public interface IConnectionProvider
    {
        /// <summary>
        /// All available connection strings.
        /// </summary>
        ConnectionStringCollection ConnectionStrings { get; }

        /// <summary>
        /// Creates an open connection for a given named connection string, using the provider name
        /// to select the proper implementation
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>The open connection</returns>
        IDbConnection OpenConnection(string name);

        /// <summary>
        /// Creates an open connection for a given connection string setting, using the provider
        /// name of select the proper implementation
        /// </summary>
        /// <param name="connectionString">ConnectionStringSetting node</param>
        /// <returns>The open connection</returns>
        IDbConnection OpenConnection(ConnectionStringSettings connectionString);
    }
}