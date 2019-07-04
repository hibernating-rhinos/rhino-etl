namespace Rhino.Etl.Core.Infrastructure
{
#if FEATURE_SQLCOMMANDSET
	// Licensed to the .NET Foundation under one or more agreements.
	// The .NET Foundation licenses this file to you under the MIT license.
	// See the LICENSE file in the project root for more information.
	using System;
	using System.Collections.Generic;
	using System.Data;
	using System.Data.Common;
	using System.Data.SqlClient;
	using System.Diagnostics;
	using System.Globalization;
	using System.Reflection;
	using System.Text.RegularExpressions;

	internal sealed class SqlCommandSet
	{
		private const string SqlIdentifierPattern = "^@[\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}\\p{Nd}\uff3f_@#\\$]*$";
		private static readonly Regex s_sqlIdentifierParser = new Regex(SqlIdentifierPattern, RegexOptions.ExplicitCapture | RegexOptions.Singleline);

		private static readonly Action<SqlCommand, bool> _setBatchRpcMode;
		private static readonly Action<SqlCommand> _clearBatchCommand;
		private static readonly Action<SqlCommand, string, SqlParameterCollection, CommandType> _addBatchCommand;
		private static readonly Func<SqlCommand, int> _executeBatchRPCCommand;

		static SqlCommandSet()
		{
			const BindingFlags BINDING_FLAGS = BindingFlags.NonPublic | BindingFlags.Instance;

			var sqlCommandType = typeof(SqlCommand);
			_setBatchRpcMode = CreateDelegate<Action<SqlCommand, bool>>(
				sqlCommandType.GetMethod("set_BatchRPCMode", BINDING_FLAGS));
			_clearBatchCommand = CreateDelegate<Action<SqlCommand>>(
				sqlCommandType.GetMethod("ClearBatchCommand", BINDING_FLAGS));
			_addBatchCommand = CreateAddBatchCommandDelegate(
				sqlCommandType.GetMethod("AddBatchCommand", BINDING_FLAGS));
			_executeBatchRPCCommand = CreateDelegate<Func<SqlCommand, int>>(
				sqlCommandType.GetMethod("ExecuteBatchRPCCommand", BINDING_FLAGS));
		}

		private static TDelegate CreateDelegate<TDelegate>(MethodInfo method)
		{
			return (TDelegate)(object)Delegate.CreateDelegate(typeof(TDelegate), method);
		}

		private static Action<SqlCommand, string, SqlParameterCollection, CommandType> CreateAddBatchCommandDelegate(MethodInfo methodInfo)
		{
			var methodParameters = methodInfo.GetParameters();
			if (methodParameters.Length == 3)
			{
				return CreateDelegate<Action<SqlCommand, string, SqlParameterCollection, CommandType>>(methodInfo);
			}

			var delegateParameterTypes = new Type[methodParameters.Length + 1];
			delegateParameterTypes[0] = typeof(SqlCommand);
			for (var i = 0; i < methodParameters.Length; i++)
			{
				delegateParameterTypes[i + 1] = methodParameters[i].ParameterType;
			}

			var delegateType = typeof(Action<,,,,>).MakeGenericType(delegateParameterTypes);
			var delegateInstance = Delegate.CreateDelegate(delegateType, methodInfo);
			var defaultColumnEncryptionValue = Enum.ToObject(methodParameters[3].ParameterType, 0);
			return (command, commandText, parameters, cmdType) => delegateInstance.DynamicInvoke(command, commandText, parameters, cmdType, defaultColumnEncryptionValue);
		}


		private List<LocalCommand> _commandList = new List<LocalCommand>();
		private SqlCommand _batchCommand;

		private sealed class LocalCommand
		{
			internal readonly string CommandText;
			internal readonly SqlParameterCollection Parameters;
			internal readonly int ReturnParameterIndex;
			internal readonly CommandType CmdType;

			internal LocalCommand(string commandText, SqlParameterCollection parameters, int returnParameterIndex, CommandType cmdType)
			{
				Debug.Assert(0 <= commandText.Length, "no text");
				CommandText = commandText;
				Parameters = parameters;
				ReturnParameterIndex = returnParameterIndex;
				CmdType = cmdType;
			}
		}

		internal SqlCommandSet()
		{
			_batchCommand = new SqlCommand();
		}

		private SqlCommand BatchCommand
		{
			get
			{
				SqlCommand command = _batchCommand;
				if (null == command)
				{
					throw ADP.ObjectDisposed(this);
				}
				return command;
			}
		}

		internal int CommandCount => CommandList.Count;

		private List<LocalCommand> CommandList
		{
			get
			{
				List<LocalCommand> commandList = _commandList;
				if (null == commandList)
				{
					throw ADP.ObjectDisposed(this);
				}
				return commandList;
			}
		}

		internal int CommandTimeout
		{
			set
			{
				BatchCommand.CommandTimeout = value;
			}
		}

		internal SqlConnection Connection
		{
			get
			{
				return BatchCommand.Connection;
			}
			set
			{
				BatchCommand.Connection = value;
			}
		}

		internal SqlTransaction Transaction
		{
			set
			{
				BatchCommand.Transaction = value;
			}
		}

		internal void Append(SqlCommand command)
		{
			ADP.CheckArgumentNull(command, nameof(command));

			string cmdText = command.CommandText;
			if (string.IsNullOrEmpty(cmdText))
			{
				throw ADP.CommandTextRequired(nameof(Append));
			}

			CommandType commandType = command.CommandType;
			switch (commandType)
			{
				case CommandType.Text:
				case CommandType.StoredProcedure:
					break;
				case CommandType.TableDirect:
					throw ADP.NotSupportedCommandType(commandType, nameof(Append));
				default:
					throw ADP.InvalidCommandType(commandType);
			}

			SqlParameterCollection parameters = null;

			SqlParameterCollection collection = command.Parameters;
			if (0 < collection.Count)
			{
				parameters = (SqlParameterCollection)Activator.CreateInstance(typeof(SqlParameterCollection), true);

				// clone parameters so they aren't destroyed
				for (int i = 0; i < collection.Count; ++i)
				{
					SqlParameter source = collection[i];
					SqlParameter p = new SqlParameter(source.ParameterName, source.SqlDbType, source.Size, source.Direction, 
						source.Precision, source.Scale, source.SourceColumn, source.SourceVersion, source.SourceColumnNullMapping, 
						source.Value, 
						NullIfEmpty(source.XmlSchemaCollectionDatabase), 
						NullIfEmpty(source.XmlSchemaCollectionOwningSchema), 
						NullIfEmpty(source.XmlSchemaCollectionName));
					parameters.Add(p);

					// SQL Injection awareness
					if (!s_sqlIdentifierParser.IsMatch(p.ParameterName))
					{
						throw ADP.BadParameterName(p.ParameterName);
					}
				}

				foreach (SqlParameter p in parameters)
				{
					// deep clone the parameter value if byte[] or char[]
					object obj = p.Value;
					byte[] byteValues = (obj as byte[]);
					if (null != byteValues)
					{
						int offset = p.Offset;
						int size = p.Size;
						int countOfBytes = byteValues.Length - offset;
						if ((0 != size) && (size < countOfBytes))
						{
							countOfBytes = size;
						}
						byte[] copy = new byte[Math.Max(countOfBytes, 0)];
						Buffer.BlockCopy(byteValues, offset, copy, 0, copy.Length);
						p.Offset = 0;
						p.Value = copy;
					}
					else
					{
						char[] charValues = (obj as char[]);
						if (null != charValues)
						{
							int offset = p.Offset;
							int size = p.Size;
							int countOfChars = charValues.Length - offset;
							if ((0 != size) && (size < countOfChars))
							{
								countOfChars = size;
							}
							char[] copy = new char[Math.Max(countOfChars, 0)];
							Buffer.BlockCopy(charValues, offset, copy, 0, copy.Length * 2);
							p.Offset = 0;
							p.Value = copy;
						}
						else
						{
							ICloneable cloneable = (obj as ICloneable);
							if (null != cloneable)
							{
								p.Value = cloneable.Clone();
							}
						}
					}
				}
			}

			int returnParameterIndex = -1;
			if (null != parameters)
			{
				for (int i = 0; i < parameters.Count; ++i)
				{
					if (ParameterDirection.ReturnValue == parameters[i].Direction)
					{
						returnParameterIndex = i;
						break;
					}
				}
			}
			LocalCommand cmd = new LocalCommand(cmdText, parameters, returnParameterIndex, command.CommandType);
			CommandList.Add(cmd);
		}

		private static string NullIfEmpty(string value)
		{
			return value == null || value.Length > 0
				? value
				: null;
		}

		//internal static void BuildStoredProcedureName(StringBuilder builder, string part)
		//{
		//	if ((null != part) && (0 < part.Length))
		//	{
		//		if ('[' == part[0])
		//		{
		//			int count = 0;
		//			foreach (char c in part)
		//			{
		//				if (']' == c)
		//				{
		//					count++;
		//				}
		//			}
		//			if (1 == (count % 2))
		//			{
		//				builder.Append(part);
		//				return;
		//			}
		//		}

		//		// the part is not escaped, escape it now
		//		SqlServerEscapeHelper.EscapeIdentifier(builder, part);
		//	}
		//}

		internal void Clear()
		{
			DbCommand batchCommand = BatchCommand;
			if (null != batchCommand)
			{
				batchCommand.Parameters.Clear();
				batchCommand.CommandText = null;
			}
			List<LocalCommand> commandList = _commandList;
			if (null != commandList)
			{
				commandList.Clear();
			}
		}

		internal void Dispose()
		{
			SqlCommand command = _batchCommand;
			_commandList = null;
			_batchCommand = null;

			if (null != command)
			{
				command.Dispose();
			}
		}

		internal int ExecuteNonQuery()
		{
			ValidateCommandBehavior(nameof(ExecuteNonQuery), CommandBehavior.Default);

			_setBatchRpcMode(BatchCommand, true);
			_clearBatchCommand(BatchCommand);
			BatchCommand.Parameters.Clear();
			for (int ii = 0; ii < _commandList.Count; ii++)
			{
				LocalCommand cmd = _commandList[ii];
				_addBatchCommand(BatchCommand, cmd.CommandText, cmd.Parameters, cmd.CmdType);
			}

			return _executeBatchRPCCommand(BatchCommand);
		}

		//internal SqlParameter GetParameter(int commandIndex, int parameterIndex)
		//	=> CommandList[commandIndex].Parameters[parameterIndex];

		//internal bool GetBatchedAffected(int commandIdentifier, out int recordsAffected, out Exception error)
		//{
		//	error = BatchCommand.GetErrors(commandIdentifier);
		//	int? affected = BatchCommand.GetRecordsAffected(commandIdentifier);
		//	recordsAffected = affected.GetValueOrDefault();
		//	return affected.HasValue;
		//}

		//internal int GetParameterCount(int commandIndex)
		//	=> CommandList[commandIndex].Parameters.Count;

		private void ValidateCommandBehavior(string method, CommandBehavior behavior)
		{
			if (0 != (behavior & ~(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection)))
			{
				ADP.ValidateCommandBehavior(behavior);
				throw ADP.NotSupportedCommandBehavior(behavior & ~(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection), method);
			}
		}

		private static class ADP
		{
			internal static ArgumentNullException ArgumentNull(string parameter)
			{
				return new ArgumentNullException(parameter);
			}

			internal static ArgumentOutOfRangeException ArgumentOutOfRange(string message, string parameterName)
			{
				return new ArgumentOutOfRangeException(parameterName, message);
			}

			internal static ArgumentException BadParameterName(string parameterName)
			{
				return new ArgumentException($"Invalid SQL parameter name '{parameterName}'.");
			}

			internal static void CheckArgumentNull(object value, string parameterName)
			{
				if (value == null) throw ArgumentNull(parameterName);
			}

			internal static Exception CommandTextRequired(string method)
			{
				return InvalidOperation($"Command text is required by method '{method}'");
			}


			internal static ArgumentOutOfRangeException InvalidCommandBehavior(CommandBehavior value)
			{
				return InvalidEnumerationValue(typeof(CommandBehavior), (int)value);
			}

			internal static ArgumentOutOfRangeException InvalidCommandType(CommandType value)
			{
				return InvalidEnumerationValue(typeof(CommandType), (int)value);
			}


			internal static ArgumentOutOfRangeException InvalidEnumerationValue(Type type, int value)
			{
				return ArgumentOutOfRange($"Value {value.ToString(CultureInfo.InvariantCulture)} is an invalid {type.Name} value", type.Name);
			}

			internal static InvalidOperationException InvalidOperation(string error)
			{
				throw new InvalidOperationException(error);
			}


			internal static ArgumentOutOfRangeException NotSupportedCommandBehavior(CommandBehavior value, string method)
			{
				return NotSupportedEnumerationValue(typeof(CommandBehavior), value.ToString(), method);
			}

			internal static ArgumentOutOfRangeException NotSupportedCommandType(CommandType value, string method)
			{
				return NotSupportedEnumerationValue(typeof(CommandType), value.ToString(), method);
			}


			internal static ArgumentOutOfRangeException NotSupportedEnumerationValue(Type type, string value, string method)
			{
				return ArgumentOutOfRange($"{type.Name} value '{value}' is not supported by method '{method}'.", type.Name);
			}

			internal static ObjectDisposedException ObjectDisposed(object instance)
			{
				return new ObjectDisposedException(instance.GetType().Name);
			}

			internal static void ValidateCommandBehavior(CommandBehavior value)
			{
				if (value < CommandBehavior.Default || (CommandBehavior.SingleResult | CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo | CommandBehavior.SingleRow | CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection) < value)
					throw InvalidCommandBehavior(value);
			}
		}
	}
#else
	#region license
	// Copyright (c) 2005 - 2007 Ayende Rahien (ayende@ayende.com)
	// All rights reserved.
	// 
	// Redistribution and use in source and binary forms, with or without modification,
	// are permitted provided that the following conditions are met:
	// 
	//     * Redistributions of source code must retain the above copyright notice,
	//     this list of conditions and the following disclaimer.
	//     * Redistributions in binary form must reproduce the above copyright notice,
	//     this list of conditions and the following disclaimer in the documentation
	//     and/or other materials provided with the distribution.
	//     * Neither the name of Ayende Rahien nor the names of its
	//     contributors may be used to endorse or promote products derived from this
	//     software without specific prior written permission.
	// 
	// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
	// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
	// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
	// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
	// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
	// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
	// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
	// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
	// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
	// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
	#endregion

    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Reflection;
    using System.Text;
    using System.Text.RegularExpressions;

	/// <summary>
	/// Expose the batch functionality in ADO.Net 2.0
	/// Microsoft in its wisdom decided to make my life hard and mark it internal.
	/// Through the use of Reflection and some delegates magic, I opened up the functionality.
	/// 
	/// There is NO documentation for this, and likely zero support.
	/// Use at your own risk, etc...
	/// 
	/// Observable performance benefits are 50%+ when used, so it is really worth it.
	/// </summary>
	public class SqlCommandSet : IDisposable
    {
        private static readonly Type sqlCmdSetType;
        private readonly object instance;
        private readonly PropSetter<SqlConnection> connectionSetter;
        private readonly PropSetter<SqlTransaction> transactionSetter;
        private readonly PropSetter<int> timeoutSetter;
        private readonly PropGetter<SqlConnection> connectionGetter;
        private readonly PropGetter<SqlCommand> commandGetter;
        private readonly AppendCommand doAppend;
        private readonly ExecuteNonQueryCommand doExecuteNonQuery;
        private readonly DisposeCommand doDispose;
        private int countOfCommands = 0;

        static SqlCommandSet()
        {
            Assembly assembly = typeof(SqlConnection).Assembly;
            sqlCmdSetType = assembly.GetType("System.Data.SqlClient.SqlCommandSet");
            Guard.Against(sqlCmdSetType == null, "Could not find SqlCommandSet!");
        }

        /// <summary>
        /// Creates a new instance of SqlCommandSet
        /// </summary>
        public SqlCommandSet()
        {

            instance = Activator.CreateInstance(sqlCmdSetType, true);

            timeoutSetter = (PropSetter<int>)
                               Delegate.CreateDelegate(typeof(PropSetter<int>),
                                                       instance, "set_CommandTimeout");
            connectionSetter = (PropSetter<SqlConnection>)
                               Delegate.CreateDelegate(typeof(PropSetter<SqlConnection>),
                                                       instance, "set_Connection");
            transactionSetter = (PropSetter<SqlTransaction>)
                                Delegate.CreateDelegate(typeof(PropSetter<SqlTransaction>),
                                                        instance, "set_Transaction");
            connectionGetter = (PropGetter<SqlConnection>)
                               Delegate.CreateDelegate(typeof(PropGetter<SqlConnection>),
                                                       instance, "get_Connection");
            commandGetter =
                (PropGetter<SqlCommand>)Delegate.CreateDelegate(typeof(PropGetter<SqlCommand>), instance, "get_BatchCommand");
            doAppend = (AppendCommand)Delegate.CreateDelegate(typeof(AppendCommand), instance, "Append");
            doExecuteNonQuery = (ExecuteNonQueryCommand)
                                Delegate.CreateDelegate(typeof(ExecuteNonQueryCommand),
                                                        instance, "ExecuteNonQuery");
            doDispose = (DisposeCommand)Delegate.CreateDelegate(typeof(DisposeCommand), instance, "Dispose");

        }

        /// <summary>
        /// Append a command to the batch
        /// </summary>
        /// <param name="command"></param>
        public void Append(SqlCommand command)
        {
            AssertHasParameters(command);
            doAppend(command);
            countOfCommands++;
        }

        /// <summary>
        /// This is required because SqlClient.SqlCommandSet will throw if 
        /// the command has no parameters.
        /// </summary>
        /// <param name="command"></param>
        private static void AssertHasParameters(SqlCommand command)
        {
            if (command.Parameters.Count == 0 &&
                (RuntimeInfo.Version.Contains("2.0") || RuntimeInfo.Version.Contains("1.1")))
            {
                throw new ArgumentException(
                    "A command in SqlCommandSet must have parameters. You can't pass hardcoded sql strings.");
            }
        }

        /// <summary>
        /// Return the batch command to be executed
        /// </summary>
        public SqlCommand BatchCommand
        {
            get
            {
                return commandGetter();
            }
        }

        /// <summary>
        /// The number of commands batched in this instance
        /// </summary>
        public int CommandCount
        {
            get { return countOfCommands; }
        }

        /// <summary>
        /// Executes the batch
        /// </summary>
        /// <returns>
        /// This seems to be returning the total number of affected rows in all queries
        /// </returns>
        public int ExecuteNonQuery()
        {
            Guard.Against<ArgumentException>(Connection == null,
                                             "Connection was not set! You must set the connection property before calling ExecuteNonQuery()");
            if(CommandCount == 0)
                return 0;
            return doExecuteNonQuery();
        }

        ///<summary>
        /// The connection the batch will use
        ///</summary>
        public SqlConnection Connection
        {
            get { return connectionGetter(); }
            set { connectionSetter(value); }
        }

        /// <summary>
        /// Set the timeout of the commandSet
        /// </summary>
        public int CommandTimeout
        {
            set { timeoutSetter(value); }
        }

        /// <summary>
        /// The transaction the batch will run as part of
        /// </summary>
        public SqlTransaction Transaction
        {
            set { transactionSetter(value); }
        }

        ///<summary>
        ///Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        ///</summary>
        ///<filterpriority>2</filterpriority>
        public void Dispose()
        {
            doDispose();
        }

	#region Delegate Definations
        private delegate void PropSetter<T>(T item);
        private delegate T PropGetter<T>();
        private delegate void AppendCommand(SqlCommand command);
        private delegate int ExecuteNonQueryCommand();
        private delegate void DisposeCommand();
	#endregion
    }
#endif
}
