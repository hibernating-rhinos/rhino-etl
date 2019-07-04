namespace Rhino.Etl.Core.Infrastructure
{
    using System;
    using System.Configuration;
    using System.Data;

    /// <summary>
    /// Helper class to provide simple data access, when we want to access the ADO.Net
    /// library directly. 
    /// </summary>
    public static class Use
    {
        #region Delegates

        /// <summary>
        /// Delegate to execute an action with a command
        /// and return a result: <typeparam name="T"/>
        /// </summary>
        [Obsolete("Use System.Func<IDbCommand, T>")]
        public delegate T Func<T>(IDbCommand command);

        /// <summary>
        /// Delegate to execute an action with a command
        /// </summary>
        [Obsolete("Use System.Proc<IDbCommand>")]
        public delegate void Proc(IDbCommand command);

        #endregion

        /// <summary>
        /// Gets or sets the active transaction.
        /// </summary>
        /// <value>The active transaction.</value>
        [ThreadStatic] 
        private static IDbTransaction ActiveTransaction;

        /// <summary>
        /// Gets or sets the transaction counter.
        /// </summary>
        /// <value>The transaction counter.</value>
        [ThreadStatic]
        private static int TransactionCounter;

        /// <summary>
        /// Gets <see cref="ConnectionStringSettings"/> for a given connection string name.
        /// </summary>
        /// <param name="name">Nameof connection string.</param>
        /// <returns>A <see cref="ConnectionStringSettings"/> instance with the given<paramref name="name"/>.</returns>
        public static ConnectionStringSettings ConnectionString(string name)
        {
            return ConnectionProvider.Default.ConnectionStrings[name];
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction and return 
        /// the result of the delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionStringName">The name of the named connection string in the configuration file</param>
        /// <param name="actionToExecute">The action to execute</param>
        /// <returns></returns>
        public static T Transaction<T>(string connectionStringName, Func<IDbCommand, T> actionToExecute)
        {
            T result = default(T);

            ConnectionStringSettings connectionStringSettings = ConnectionString(connectionStringName);
            if (connectionStringSettings == null)
                throw new InvalidOperationException("Could not find connnection string: " + connectionStringName);

            Transaction(connectionStringSettings, delegate(IDbCommand command) { result = actionToExecute(command); });
            return result;
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction and return 
        /// the result of the delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionStringSettings">The connection string settings to use for the connection</param>
        /// <param name="actionToExecute">The action to execute</param>
        /// <returns></returns>
        public static T Transaction<T>(ConnectionStringSettings connectionStringSettings, Func<IDbCommand, T> actionToExecute)
        {
            T result = default(T);
            Transaction(connectionStringSettings, delegate(IDbCommand command) { result = actionToExecute(command); });
            return result;
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        /// <param name="actionToExecute">The action to execute.</param>
        public static void Transaction(string connectionStringName, Action<IDbCommand> actionToExecute)
        {
            ConnectionStringSettings connectionStringSettings = ConnectionString(connectionStringName);
            if (connectionStringSettings == null)
                throw new InvalidOperationException("Could not find connnection string: " + connectionStringName);

            Transaction(connectionStringSettings, IsolationLevel.Unspecified, actionToExecute);
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction
        /// </summary>
        /// <param name="connectionStringSettings">The connection string settings to use for the connection</param>
        /// <param name="actionToExecute">The action to execute.</param>
        public static void Transaction(ConnectionStringSettings connectionStringSettings, Action<IDbCommand> actionToExecute)
        {
            Transaction(connectionStringSettings, IsolationLevel.Unspecified, actionToExecute);
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction with the specific
        /// isolation level 
        /// </summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        /// <param name="isolationLevel">The isolation level.</param>
        /// <param name="actionToExecute">The action to execute.</param>
        public static void Transaction(string connectionStringName, IsolationLevel isolationLevel, Action<IDbCommand> actionToExecute)
        {
            ConnectionStringSettings connectionStringSettings = ConnectionString(connectionStringName);
            if (connectionStringSettings == null)
                throw new InvalidOperationException("Could not find connnection string: " + connectionStringName);

            Transaction(connectionStringSettings, isolationLevel, actionToExecute);
        }

        /// <summary>
        /// Execute the specified delegate inside a transaction with the specific
        /// isolation level 
        /// </summary>
        /// <param name="connectionStringSettings">Connection string settings node to use for the connection</param>
        /// <param name="isolationLevel">The isolation level.</param>
        /// <param name="actionToExecute">The action to execute.</param>
        public static void Transaction(ConnectionStringSettings connectionStringSettings, IsolationLevel isolationLevel, Action<IDbCommand> actionToExecute)
        {
            StartTransaction(connectionStringSettings, isolationLevel);
            try
            {
                var transaction = ActiveTransaction;
                using (IDbCommand command = transaction.Connection.CreateCommand())
                {
                    command.Transaction = transaction;
                    actionToExecute(command);
                }
                CommitTransaction();
            }
            catch
            {
                RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// Rollbacks the transaction.
        /// </summary>
        private static void RollbackTransaction()
        {
            var transaction = ActiveTransaction;
            if (transaction != null)
            {
                var connection = transaction.Connection;
                try
                {
                    transaction.Rollback();
                }
                finally
                {
                    ActiveTransaction = null;
                    TransactionCounter = 0;

                    transaction.Dispose();
                    connection.Dispose();
                }
            }
        }

        /// <summary>
        /// Commits the transaction.
        /// </summary>
        private static void CommitTransaction()
        {
            TransactionCounter--;
            if (TransactionCounter != 0) return;

            var transaction = ActiveTransaction;
            if (transaction != null)
            {
                ActiveTransaction = null;

                var connection = transaction.Connection;
                try
                {
                    transaction.Commit();
                }
                finally
                {
                    transaction.Dispose();
                    connection.Dispose();
                }
            }
        }

        /// <summary>
        /// Starts the transaction.
        /// </summary>
        /// <param name="connectionStringSettings">The connection string settings to use for the transaction</param>
        /// <param name="isolation">The isolation.</param>
        private static void StartTransaction(ConnectionStringSettings connectionStringSettings, IsolationLevel isolation)
        {
            if (TransactionCounter <= 0)
            {
                TransactionCounter = 0;
                var connection = Connection(connectionStringSettings);
                ActiveTransaction = connection.BeginTransaction(isolation);
            }
            TransactionCounter++;
        }

        /// <summary>
        /// Creates an open connection for a given named connection string, using the provider name
        /// to select the proper implementation
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>The open connection</returns>
        public static IDbConnection Connection(string name)
        {
            return ConnectionProvider.Default.OpenConnection(name);
        }

        /// <summary>
        /// Creates an open connection for a given connection string setting, using the provider
        /// name of select the proper implementation
        /// </summary>
        /// <param name="connectionString">ConnectionStringSetting node</param>
        /// <returns>The open connection</returns>
        public static IDbConnection Connection(ConnectionStringSettings connectionString)
        {
            return ConnectionProvider.Default.OpenConnection(connectionString);
        }
    }
}