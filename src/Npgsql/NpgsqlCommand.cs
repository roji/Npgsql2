using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.TypeMapping;
using Npgsql.Util;
using NpgsqlTypes;
using static Npgsql.Util.Statics;
using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Npgsql
{
    /// <summary>
    /// Represents a SQL statement or function (stored procedure) to execute
    /// against a PostgreSQL database. This class cannot be inherited.
    /// </summary>
    // ReSharper disable once RedundantNameQualifier
    [System.ComponentModel.DesignerCategory("")]
    public sealed class NpgsqlCommand : DbCommand, IExecutable, ICloneable, IComponent
    {
        #region Fields

        NpgsqlConnection? _connection;

        /// <summary>
        /// If this command is (explicitly) prepared, references the connector on which the preparation happened.
        /// Used to detect when the connector was changed (i.e. connection open/close), meaning that the command
        /// is no longer prepared.
        /// </summary>
        NpgsqlConnector? _connectorPreparedOn;

        string? _commandText, _rewrittenCommandText;
        CommandBehavior _behavior;
        int? _timeout;
        readonly NpgsqlParameterCollection _parameters = new();

        NpgsqlBatch? _preparedBatch;

        /// <summary>
        /// Returns details about each statement that this command has executed.
        /// Is only populated when an Execute* method is called.
        /// </summary>
        public IReadOnlyList<NpgsqlStatement> Statements => throw new NotImplementedException();

        /// <summary>
        /// If this command has been prepared, references the <see cref="PreparedStatement"/>, otherwise <see langword="null" />.
        /// </summary>
        internal PreparedStatement? PreparedStatement
        {
            get => _preparedStatement != null && _preparedStatement.State == PreparedState.Unprepared
                ? _preparedStatement = null
                : _preparedStatement;
            set => _preparedStatement = value;
        }

        PreparedStatement? _preparedStatement;

        // TODO: See about killing this
        internal bool IsPreparing;

        /// <summary>
        /// The RowDescription message for this query. If null, the query does not return rows (e.g. INSERT)
        /// </summary>
        internal RowDescriptionMessage? Description
        {
            get => PreparedStatement == null ? _description : PreparedStatement.Description;
            set
            {
                if (PreparedStatement == null)
                    _description = value;
                else
                    PreparedStatement.Description = value;
            }
        }

        RowDescriptionMessage? _description;

        UpdateRowSource _updateRowSource = UpdateRowSource.Both;

        bool IsExplicitlyPrepared => _connectorPreparedOn != null;

        static readonly List<NpgsqlParameter> EmptyParameters = new();

        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlCommand));

        #endregion Fields

        #region Constants

        internal const int DefaultTimeout = 30;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand"/> class.
        /// </summary>
        public NpgsqlCommand()
            => GC.SuppressFinalize(this);

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand"/> class with the text of the query.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        // ReSharper disable once IntroduceOptionalParameters.Global
        public NpgsqlCommand(string? cmdText)
        {
            GC.SuppressFinalize(this);
            _commandText = cmdText;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand"/> class with the text of the query and a
        /// <see cref="NpgsqlConnection"/>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="NpgsqlConnection"/> that represents the connection to a PostgreSQL server.</param>
        // ReSharper disable once IntroduceOptionalParameters.Global
        public NpgsqlCommand(string? cmdText, NpgsqlConnection? connection)
        {
            GC.SuppressFinalize(this);
            _commandText = cmdText;
            _connection = connection;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlCommand"/> class with the text of the query, a
        /// <see cref="NpgsqlConnection"/>, and the <see cref="NpgsqlTransaction"/>.
        /// </summary>
        /// <param name="cmdText">The text of the query.</param>
        /// <param name="connection">A <see cref="NpgsqlConnection"/> that represents the connection to a PostgreSQL server.</param>
        /// <param name="transaction">The <see cref="NpgsqlTransaction"/> in which the <see cref="NpgsqlCommand"/> executes.</param>
        public NpgsqlCommand(string? cmdText, NpgsqlConnection? connection, NpgsqlTransaction? transaction)
        {
            GC.SuppressFinalize(this);
            _commandText = cmdText;
            _connection = connection;
            Transaction = transaction;
        }

        #endregion Constructors

        #region Public properties

        /// <summary>
        /// Gets or sets the SQL statement or function (stored procedure) to execute at the data source.
        /// </summary>
        /// <value>The Transact-SQL statement or stored procedure to execute. The default is an empty string.</value>
        [AllowNull, DefaultValue("")]
        [Category("Data")]
        public override string CommandText
        {
            get => _commandText ?? string.Empty;
            set
            {
                _commandText = State == CommandState.Idle
                    ? value
                    : throw new InvalidOperationException("An open data reader exists for this command.");

                ResetExplicitPreparation();
                // TODO: Technically should do this also if the parameter list (or type) changes
            }
        }

        /// <summary>
        /// Gets or sets the wait time (in seconds) before terminating the attempt  to execute a command and generating an error.
        /// </summary>
        /// <value>The time (in seconds) to wait for the command to execute. The default value is 30 seconds.</value>
        [DefaultValue(DefaultTimeout)]
        public override int CommandTimeout
        {
            get => _timeout ?? (_connection?.CommandTimeout ?? DefaultTimeout);
            set
            {
                if (value < 0) {
                    throw new ArgumentOutOfRangeException(nameof(value), value, "CommandTimeout can't be less than zero.");
                }

                _timeout = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how the <see cref="NpgsqlCommand.CommandText"/> property is to be interpreted.
        /// </summary>
        /// <value>
        /// One of the <see cref="System.Data.CommandType"/> values. The default is <see cref="System.Data.CommandType.Text"/>.
        /// </value>
        [DefaultValue(CommandType.Text)]
        [Category("Data")]
        public override CommandType CommandType { get; set; } = CommandType.Text;

        /// <summary>
        /// DB connection.
        /// </summary>
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => _connection = (NpgsqlConnection?)value;
        }

        /// <summary>
        /// Gets or sets the <see cref="NpgsqlConnection"/> used by this instance of the <see cref="NpgsqlCommand"/>.
        /// </summary>
        /// <value>The connection to a data source. The default value is <see langword="null"/>.</value>
        [DefaultValue(null)]
        [Category("Behavior")]
        public new NpgsqlConnection? Connection
        {
            get => _connection;
            set
            {
                if (_connection == value)
                    return;

                _connection = State == CommandState.Idle
                    ? value
                    : throw new InvalidOperationException("An open data reader exists for this command.");

                Transaction = null;
            }
        }

        /// <summary>
        /// Design time visible.
        /// </summary>
        public override bool DesignTimeVisible { get; set; }

        /// <summary>
        /// Gets or sets how command results are applied to the DataRow when used by the
        /// DbDataAdapter.Update(DataSet) method.
        /// </summary>
        /// <value>One of the <see cref="System.Data.UpdateRowSource"/> values.</value>
        [Category("Behavior"), DefaultValue(UpdateRowSource.Both)]
        public override UpdateRowSource UpdatedRowSource
        {
            get => _updateRowSource;
            set
            {
                switch (value)
                {
                // validate value (required based on base type contract)
                case UpdateRowSource.None:
                case UpdateRowSource.OutputParameters:
                case UpdateRowSource.FirstReturnedRecord:
                case UpdateRowSource.Both:
                    _updateRowSource = value;
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
                }
            }
        }

        /// <summary>
        /// Returns whether this query will execute as a prepared (compiled) query.
        /// </summary>
        public bool IsPrepared
            => _connectorPreparedOn == _connection?.Connector && PreparedStatement?.IsPrepared == true;

        #endregion Public properties

        #region Known/unknown Result Types Management

        /// <summary>
        /// Marks all of the query's result columns as either known or unknown.
        /// Unknown results column are requested them from PostgreSQL in text format, and Npgsql makes no
        /// attempt to parse them. They will be accessible as strings only.
        /// </summary>
        public bool AllResultTypesAreUnknown
        {
            get => _allResultTypesAreUnknown;
            set
            {
                // TODO: Check that this isn't modified after calling prepare
                _unknownResultTypeList = null;
                _allResultTypesAreUnknown = value;
            }
        }

        bool _allResultTypesAreUnknown;

        /// <summary>
        /// Marks the query's result columns as known or unknown, on a column-by-column basis.
        /// Unknown results column are requested them from PostgreSQL in text format, and Npgsql makes no
        /// attempt to parse them. They will be accessible as strings only.
        /// </summary>
        /// <remarks>
        /// If the query includes several queries (e.g. SELECT 1; SELECT 2), this will only apply to the first
        /// one. The rest of the queries will be fetched and parsed as usual.
        ///
        /// The array size must correspond exactly to the number of result columns the query returns, or an
        /// error will be raised.
        /// </remarks>
        public bool[]? UnknownResultTypeList
        {
            get => _unknownResultTypeList;
            set
            {
                // TODO: Check that this isn't modified after calling prepare
                _allResultTypesAreUnknown = false;
                _unknownResultTypeList = value;
            }
        }

        bool[]? _unknownResultTypeList;

        #endregion

        #region Result Types Management

        /// <summary>
        /// Marks result types to be used when using GetValue on a data reader, on a column-by-column basis.
        /// Used for Entity Framework 5-6 compability.
        /// Only primitive numerical types and DateTimeOffset are supported.
        /// Set the whole array or just a value to null to use default type.
        /// </summary>
        internal Type[]? ObjectResultTypes { get; set; }

        Type[]? IExecutable.ObjectResultTypes => ObjectResultTypes;

        #endregion

        #region State management

        volatile int _state;

        /// <summary>
        /// The current state of the command
        /// </summary>
        internal CommandState State
        {
            get => (CommandState)_state;
            set
            {
                var newState = (int)value;
                if (newState == _state)
                    return;
                _state = newState;
            }
        }

        // TODO: Revisit
        CommandState IExecutable.State
        {
            set => State = value;
        }

        void ResetExplicitPreparation()
        {
            PreparedStatement = null;
            _connectorPreparedOn = null;
            _preparedBatch = null;
        }

        #endregion State management

        #region Parameters

        /// <summary>
        /// Creates a new instance of an <see cref="System.Data.Common.DbParameter"/> object.
        /// </summary>
        /// <returns>A <see cref="System.Data.Common.DbParameter"/> object.</returns>
        protected override DbParameter CreateDbParameter() => CreateParameter();

        /// <summary>
        /// Creates a new instance of a <see cref="NpgsqlParameter"/> object.
        /// </summary>
        /// <returns>An <see cref="NpgsqlParameter"/> object.</returns>
        public new NpgsqlParameter CreateParameter() => new();

        /// <summary>
        /// DB parameter collection.
        /// </summary>
        protected override DbParameterCollection DbParameterCollection => Parameters;

        /// <summary>
        /// Gets the <see cref="NpgsqlParameterCollection"/>.
        /// </summary>
        /// <value>The parameters of the SQL statement or function (stored procedure). The default is an empty collection.</value>
        public new NpgsqlParameterCollection Parameters => _parameters;

        /// <summary>
        /// The input parameters sent with this statement.
        /// </summary>
        internal List<NpgsqlParameter> InputParameters { get; } = new();

        #endregion

        #region DeriveParameters

        const string DeriveParametersForFunctionQuery = @"
SELECT
CASE
	WHEN pg_proc.proargnames IS NULL THEN array_cat(array_fill(''::name,ARRAY[pg_proc.pronargs]),array_agg(pg_attribute.attname ORDER BY pg_attribute.attnum))
	ELSE pg_proc.proargnames
END AS proargnames,
pg_proc.proargtypes,
CASE
	WHEN pg_proc.proallargtypes IS NULL AND (array_agg(pg_attribute.atttypid))[1] IS NOT NULL THEN array_cat(string_to_array(pg_proc.proargtypes::text,' ')::oid[],array_agg(pg_attribute.atttypid ORDER BY pg_attribute.attnum))
	ELSE pg_proc.proallargtypes
END AS proallargtypes,
CASE
	WHEN pg_proc.proargmodes IS NULL AND (array_agg(pg_attribute.atttypid))[1] IS NOT NULL THEN array_cat(array_fill('i'::""char"",ARRAY[pg_proc.pronargs]),array_fill('o'::""char"",ARRAY[array_length(array_agg(pg_attribute.atttypid), 1)]))
    ELSE pg_proc.proargmodes
END AS proargmodes
FROM pg_proc
LEFT JOIN pg_type ON pg_proc.prorettype = pg_type.oid
LEFT JOIN pg_attribute ON pg_type.typrelid = pg_attribute.attrelid AND pg_attribute.attnum >= 1 AND NOT pg_attribute.attisdropped
WHERE pg_proc.oid = :proname::regproc
GROUP BY pg_proc.proargnames, pg_proc.proargtypes, pg_proc.proallargtypes, pg_proc.proargmodes, pg_proc.pronargs;
";

        internal void DeriveParameters()
        {
            var conn = CheckAndGetConnection();

            using var _ = conn.StartTemporaryBindingScope(out var connector);

            if (Statements.Any(s => s.PreparedStatement?.IsExplicit == true))
                throw new NpgsqlException("Deriving parameters isn't supported for commands that are already prepared.");

            // Here we unprepare statements that possibly are auto-prepared
            Unprepare();

            Parameters.Clear();

            switch (CommandType)
            {
            case CommandType.Text:
                DeriveParametersForQuery(connector);
                break;
            case CommandType.StoredProcedure:
                DeriveParametersForFunction();
                break;
            default:
                throw new NotSupportedException("Cannot derive parameters for CommandType " + CommandType);
            }
        }

        void DeriveParametersForFunction()
        {
            using var c = new NpgsqlCommand(DeriveParametersForFunctionQuery, _connection);
            c.Parameters.Add(new NpgsqlParameter("proname", NpgsqlDbType.Text));
            c.Parameters[0].Value = CommandText;

            string[]? names = null;
            uint[]? types = null;
            char[]? modes = null;

            using (var rdr = c.ExecuteReader(CommandBehavior.SingleRow | CommandBehavior.SingleResult))
            {
                if (rdr.Read())
                {
                    if (!rdr.IsDBNull(0))
                        names = rdr.GetFieldValue<string[]>(0);
                    if (!rdr.IsDBNull(2))
                        types = rdr.GetFieldValue<uint[]>(2);
                    if (!rdr.IsDBNull(3))
                        modes = rdr.GetFieldValue<char[]>(3);
                    if (types == null)
                    {
                        if (rdr.IsDBNull(1) || rdr.GetFieldValue<uint[]>(1).Length == 0)
                            return;  // Parameter-less function
                        types = rdr.GetFieldValue<uint[]>(1);
                    }
                }
                else
                    throw new InvalidOperationException($"{CommandText} does not exist in pg_proc");
            }

            var typeMapper = c._connection!.Connector!.TypeMapper;

            for (var i = 0; i < types.Length; i++)
            {
                var param = new NpgsqlParameter();

                var (npgsqlDbType, postgresType) = typeMapper.GetTypeInfoByOid(types[i]);

                param.DataTypeName = postgresType.DisplayName;
                param.PostgresType = postgresType;
                if (npgsqlDbType.HasValue)
                    param.NpgsqlDbType = npgsqlDbType.Value;

                if (names != null && i < names.Length)
                    param.ParameterName = names[i];
                else
                    param.ParameterName = "parameter" + (i + 1);

                if (modes == null) // All params are IN, or server < 8.1.0 (and only IN is supported)
                    param.Direction = ParameterDirection.Input;
                else
                {
                    param.Direction = modes[i] switch
                    {
                        'i' => ParameterDirection.Input,
                        'o' => ParameterDirection.Output,
                        't' => ParameterDirection.Output,
                        'b' => ParameterDirection.InputOutput,
                        'v' => throw new NotSupportedException("Cannot derive function parameter of type VARIADIC"),
                        _ => throw new ArgumentOutOfRangeException("Unknown code in proargmodes while deriving: " + modes[i])
                    };
                }

                Parameters.Add(param);
            }
        }

        void DeriveParametersForQuery(NpgsqlConnector connector)
        {
            throw new NotImplementedException();
            // using (connector.StartUserAction())
            // {
            //     Log.Debug($"Deriving Parameters for query: {CommandText}", connector.Id);
            //     ProcessUserQuery(connector.UseConformingStrings, deriveParameters: true);
            //
            //     var sendTask = SendDeriveParameters(connector, false);
            //     if (sendTask.IsFaulted)
            //         sendTask.GetAwaiter().GetResult();
            //
            //     foreach (var statement in _statements)
            //     {
            //         Expect<ParseCompleteMessage>(
            //             connector.ReadMessage(async: false).GetAwaiter().GetResult(), connector);
            //         var paramTypeOIDs = Expect<ParameterDescriptionMessage>(
            //             connector.ReadMessage(async: false).GetAwaiter().GetResult(), connector).TypeOIDs;
            //
            //         if (statement.InputParameters.Count != paramTypeOIDs.Count)
            //         {
            //             connector.SkipUntil(BackendMessageCode.ReadyForQuery);
            //             Parameters.Clear();
            //             throw new NpgsqlException("There was a mismatch in the number of derived parameters between the Npgsql SQL parser and the PostgreSQL parser. Please report this as bug to the Npgsql developers (https://github.com/npgsql/npgsql/issues).");
            //         }
            //
            //         for (var i = 0; i < paramTypeOIDs.Count; i++)
            //         {
            //             try
            //             {
            //                 var param = statement.InputParameters[i];
            //                 var paramOid = paramTypeOIDs[i];
            //
            //                 var (npgsqlDbType, postgresType) = connector.TypeMapper.GetTypeInfoByOid(paramOid);
            //
            //                 if (param.NpgsqlDbType != NpgsqlDbType.Unknown && param.NpgsqlDbType != npgsqlDbType)
            //                     throw new NpgsqlException("The backend parser inferred different types for parameters with the same name. Please try explicit casting within your SQL statement or batch or use different placeholder names.");
            //
            //                 param.DataTypeName = postgresType.DisplayName;
            //                 param.PostgresType = postgresType;
            //                 if (npgsqlDbType.HasValue)
            //                     param.NpgsqlDbType = npgsqlDbType.Value;
            //             }
            //             catch
            //             {
            //                 connector.SkipUntil(BackendMessageCode.ReadyForQuery);
            //                 Parameters.Clear();
            //                 throw;
            //             }
            //         }
            //
            //         var msg = connector.ReadMessage(async: false).GetAwaiter().GetResult();
            //         switch (msg.Code)
            //         {
            //         case BackendMessageCode.RowDescription:
            //         case BackendMessageCode.NoData:
            //             break;
            //         default:
            //             throw connector.UnexpectedMessageReceived(msg.Code);
            //         }
            //     }
            //
            //     Expect<ReadyForQueryMessage>(connector.ReadMessage(async: false).GetAwaiter().GetResult(), connector);
            //     sendTask.GetAwaiter().GetResult();
            // }
        }

        #endregion

        #region Prepare

        /// <summary>
        /// Creates a server-side prepared statement on the PostgreSQL server.
        /// This will make repeated future executions of this command much faster.
        /// </summary>
        public override void Prepare() => Prepare(false).GetAwaiter().GetResult();

        /// <summary>
        /// Creates a server-side prepared statement on the PostgreSQL server.
        /// This will make repeated future executions of this command much faster.
        /// </summary>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
#if NETSTANDARD2_0
        public Task PrepareAsync(CancellationToken cancellationToken = default)
#else
        public override Task PrepareAsync(CancellationToken cancellationToken = default)
#endif
            => Prepare(true, cancellationToken);

        async Task Prepare(bool async, CancellationToken cancellationToken = default)
        {
            var connection = CheckAndGetConnection();

            if (IsPrepared)
                return;

            NpgsqlBatchCommandCollection? batchCommands = null;

            var commandRewriter = new CommandRewriter();
            commandRewriter.RewriteCommand(
                CommandText,
                CommandType,
                Parameters,
                out _rewrittenCommandText,
                InputParameters,
                ref batchCommands,
                connection.Connector!.UseConformingStrings);

            if (batchCommands is null)
            {
                // The command contained a single statement.
                // We still do preparation via a batch to reduce code duplication.
                batchCommands = new NpgsqlBatchCommandCollection
                {
                    new(_rewrittenCommandText, CommandType, InputParameters)
                };

                var batch = new NpgsqlBatch(connection, batchCommands);
                await batch.Prepare(async, cancellationToken);

                PreparedStatement = batch.BatchCommands[0].PreparedStatement;
                _connectorPreparedOn = connection.Connector;
            }
            else
            {
                // The command contained multiple statements.
                // We prepare the batch and save it for use when the command is executed later.
                _preparedBatch = new NpgsqlBatch(connection, batchCommands);
                await _preparedBatch.Prepare(async, cancellationToken);
                _connectorPreparedOn = connection.Connector;
            }
        }

        /// <summary>
        /// Unprepares a command, closing server-side statements associated with it.
        /// Note that this only affects commands explicitly prepared with <see cref="Prepare()"/>, not
        /// automatically prepared statements.
        /// </summary>
        public void Unprepare()
            => Unprepare(false).GetAwaiter().GetResult();

        /// <summary>
        /// Unprepares a command, closing server-side statements associated with it.
        /// Note that this only affects commands explicitly prepared with <see cref="Prepare()"/>, not
        /// automatically prepared statements.
        /// </summary>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        public Task UnprepareAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return Unprepare(true, cancellationToken);
        }

        Task Unprepare(bool async, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
            // var connection = CheckAndGetConnection();
            // if (connection.Settings.Multiplexing)
            //     throw new NotSupportedException("Explicit preparation not supported with multiplexing");
            // if (_statements.All(s => !s.IsPrepared))
            //     return;
            //
            // var connector = connection.Connector!;
            //
            // Log.Debug("Closing command's prepared statements", connector.Id);
            // using (connector.StartUserAction(cancellationToken))
            // {
            //     var sendTask = SendClose(connector, async, cancellationToken);
            //     if (sendTask.IsFaulted)
            //         sendTask.GetAwaiter().GetResult();
            //     foreach (var statement in _statements)
            //         if (statement.PreparedStatement?.State == PreparedState.BeingUnprepared)
            //         {
            //             Expect<CloseCompletedMessage>(await connector.ReadMessage(async), connector);
            //             statement.PreparedStatement.CompleteUnprepare();
            //             statement.PreparedStatement = null;
            //         }
            //     Expect<ReadyForQueryMessage>(await connector.ReadMessage(async), connector);
            //     if (async)
            //         await sendTask;
            //     else
            //         sendTask.GetAwaiter().GetResult();
            // }
        }

        #endregion Prepare

        #region Execute

        void ValidateParameters(ConnectorTypeMapper typeMapper)
        {
            for (var i = 0; i < Parameters.Count; i++)
            {
                var p = Parameters[i];
                if (!p.IsInputDirection)
                    continue;
                p.Bind(typeMapper);
                p.LengthCache?.Clear();
                p.ValidateAndGetLength();
            }
        }

        #endregion

        #region Message Creation / Population

        async Task SendExecute(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default)
        {
            // TODO: See about factoring this out to ExecutionHelper. That would mean an extra async state machine for batching.
            // TODO: If we keep these separate implementations (not good), we can remove IsPreparing.
            Debug.Assert(_rewrittenCommandText is not null);

            var pStatement = PreparedStatement;

            if (_behavior.HasFlag(CommandBehavior.SchemaOnly))
            {
                if (pStatement?.State == PreparedState.Prepared)
                    return; // Prepared, we already have the RowDescription

                Debug.Assert(pStatement == null);

                await connector.WriteParse(_rewrittenCommandText, string.Empty, InputParameters, async, cancellationToken);
                await connector.WriteDescribe(StatementOrPortal.Statement, string.Empty, async, cancellationToken);
            }
            else
            {
                var statementName = pStatement is null ? "" : pStatement.Name;
                Debug.Assert(statementName is not null);

                if (pStatement == null || IsPreparing)
                {
                    // The statement should either execute unprepared, or is being auto-prepared.
                    // Send Parse, Bind, Describe

                    // We may have a prepared statement that replaces an existing statement - close the latter first.
                    if (pStatement?.StatementBeingReplaced != null)
                        await connector.WriteClose(StatementOrPortal.Statement, pStatement.StatementBeingReplaced.Name!, async, cancellationToken);

                    await connector.WriteParse(_rewrittenCommandText, statementName, InputParameters, async, cancellationToken);

                    await connector.WriteBind(
                        InputParameters, string.Empty, statementName, AllResultTypesAreUnknown,
                        UnknownResultTypeList, async, cancellationToken);

                    await connector.WriteDescribe(StatementOrPortal.Portal, string.Empty, async, cancellationToken);
                }
                else
                {
                    // The statement is already prepared, only a Bind is needed
                    await connector.WriteBind(
                        InputParameters, string.Empty, statementName, AllResultTypesAreUnknown,
                        UnknownResultTypeList, async, cancellationToken);
                }

                await connector.WriteExecute(0, async, cancellationToken);

                if (pStatement != null)
                    pStatement.LastUsed = DateTime.UtcNow;
            }

            await connector.WriteSync(async, cancellationToken);
        }

        Task IExecutable.SendExecute(NpgsqlConnector connector, bool async, CancellationToken cancellationToken)
            => SendExecute(connector, async, cancellationToken);

        Task SendDeriveParameters(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
            // BeginSend(connector);

            // for (var i = 0; i < _statements.Count; i++)
            // {
            //     // async = ForceAsyncIfNecessary(async, i);
            //
            //     var statement = _statements[i];
            //
            //     await connector.WriteParse(statement.SQL, string.Empty, EmptyParameters, async, cancellationToken);
            //     await connector.WriteDescribe(StatementOrPortal.Statement, string.Empty, async, cancellationToken);
            // }
            //
            // await connector.WriteSync(async, cancellationToken);
            // await connector.Flush(async, cancellationToken);

            // CleanupSend();
        }

        async Task SendPrepare(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default)
        {
            Debug.Assert(_rewrittenCommandText is not null);

            var pStatement = PreparedStatement;

            // A statement may be already prepared, already in preparation (i.e. same statement twice
            // in the same command), or we can't prepare (overloaded SQL)
            if (!IsPreparing)
                return;

            // We may have a prepared statement that replaces an existing statement - close the latter first.
            var statementToClose = pStatement!.StatementBeingReplaced;
            if (statementToClose != null)
                await connector.WriteClose(StatementOrPortal.Statement, statementToClose.Name!, async, cancellationToken);

            await connector.WriteParse(_rewrittenCommandText, pStatement.Name!, InputParameters, async, cancellationToken);
            await connector.WriteDescribe(StatementOrPortal.Statement, pStatement.Name!, async, cancellationToken);
        }

        Task SendClose(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
            // BeginSend(connector);

            // foreach (var statement in _statements.Where(s => s.IsPrepared))
            // {
            //     // if (FlushOccurred)
            //     // {
            //     //     async = true;
            //     //     SynchronizationContext.SetSynchronizationContext(SingleThreadSynchronizationContext.Instance);
            //     // }
            //
            //     await connector.WriteClose(StatementOrPortal.Statement, statement.StatementName, async, cancellationToken);
            //     statement.PreparedStatement!.State = PreparedState.BeingUnprepared;
            // }
            //
            // await connector.WriteSync(async, cancellationToken);
            // await connector.Flush(async, cancellationToken);

            // CleanupSend();
        }

        #endregion

        #region Execute Non Query

        /// <summary>
        /// Executes a SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <returns>The number of rows affected if known; -1 otherwise.</returns>
        public override int ExecuteNonQuery() => ExecuteNonQuery(false, CancellationToken.None).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous version of <see cref="ExecuteNonQuery()"/>
        /// </summary>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>A task representing the asynchronous operation, with the number of rows affected if known; -1 otherwise.</returns>
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            using (NoSynchronizationContextScope.Enter())
                return ExecuteNonQuery(true, cancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        async Task<int> ExecuteNonQuery(bool async, CancellationToken cancellationToken)
        {
            using var reader = await ExecuteReader(CommandBehavior.Default, async, cancellationToken);
            while (async ? await reader.NextResultAsync(cancellationToken) : reader.NextResult()) ;

            return reader.RecordsAffected;
        }

        #endregion Execute Non Query

        #region Execute Scalar

        /// <summary>
        /// Executes the query, and returns the first column of the first row
        /// in the result set returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns>The first column of the first row in the result set,
        /// or a null reference if the result set is empty.</returns>
        public override object? ExecuteScalar() => ExecuteScalar(false, CancellationToken.None).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous version of <see cref="ExecuteScalar()"/>
        /// </summary>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>A task representing the asynchronous operation, with the first column of the
        /// first row in the result set, or a null reference if the result set is empty.</returns>
        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            using (NoSynchronizationContextScope.Enter())
                return ExecuteScalar(true, cancellationToken).AsTask();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        async ValueTask<object?> ExecuteScalar(bool async, CancellationToken cancellationToken)
        {
            var behavior = CommandBehavior.SingleRow;
            if (!Parameters.HasOutputParameters)
                behavior |= CommandBehavior.SequentialAccess;

            using var reader = await ExecuteReader(behavior, async, cancellationToken);
            return reader.Read() && reader.FieldCount != 0 ? reader.GetValue(0) : null;
        }

        #endregion Execute Scalar

        #region Execute Reader

        /// <summary>
        /// Executes the command text against the connection.
        /// </summary>
        /// <returns>A task representing the operation.</returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => ExecuteReader(behavior);

        /// <summary>
        /// Executes the command text against the connection.
        /// </summary>
        /// <param name="behavior">An instance of <see cref="CommandBehavior"/>.</param>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
            => await ExecuteReaderAsync(behavior, cancellationToken);

        /// <summary>
        /// Executes the <see cref="CommandText"/> against the <see cref="Connection"/>
        /// and returns a <see cref="NpgsqlDataReader"/>.
        /// </summary>
        /// <param name="behavior">One of the enumeration values that specified the command behavior.</param>
        /// <returns>A task representing the operation.</returns>
        public new NpgsqlDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
            => ExecuteReader(behavior, async: false, CancellationToken.None).GetAwaiter().GetResult();

        /// <summary>
        /// An asynchronous version of <see cref="ExecuteReader(CommandBehavior)"/>, which executes
        /// the <see cref="CommandText"/> against the <see cref="Connection"/>
        /// and returns a <see cref="NpgsqlDataReader"/>.
        /// </summary>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public new Task<NpgsqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
            => ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

        /// <summary>
        /// An asynchronous version of <see cref="ExecuteReader(CommandBehavior)"/>,
        /// which executes the <see cref="CommandText"/> against the <see cref="Connection"/>
        /// and returns a <see cref="NpgsqlDataReader"/>.
        /// </summary>
        /// <param name="behavior">One of the enumeration values that specified the command behavior.</param>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public new Task<NpgsqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return ExecuteReader(behavior, async: true, cancellationToken).AsTask();
        }

        // TODO: Maybe pool these?
        ManualResetValueTaskSource<NpgsqlConnector> ExecutionCompletion { get; } = new();
        ManualResetValueTaskSource<NpgsqlConnector> IExecutable.ExecutionCompletion => ExecutionCompletion;

        internal async ValueTask<NpgsqlDataReader> ExecuteReader(CommandBehavior behavior, bool async, CancellationToken cancellationToken)
        {
            var conn = CheckAndGetConnection();
            _behavior = behavior;

            try
            {
                if (conn.TryGetBoundConnector(out var connector))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    ValidateParameters(connector.TypeMapper);

                    switch (IsExplicitlyPrepared)
                    {
                    case true:
                        Debug.Assert(_connectorPreparedOn != null);
                        if (_connectorPreparedOn != connector)
                        {
                            // The command was prepared, but since then the connector has changed. Detach all prepared statements.
                            ResetExplicitPreparation();
                            goto case false;
                        }

                        if (_preparedBatch is not null)
                        {
                            return await _preparedBatch.ExecuteReader(async, cancellationToken);
                        }

                        NpgsqlEventSource.Log.CommandStartPrepared();
                        break;

                    case false:
                        // TODO: Recycle
                        NpgsqlBatchCommandCollection? batchCommands = null;

                        var commandRewriter = new CommandRewriter();
                        commandRewriter.RewriteCommand(
                            CommandText,
                            CommandType,
                            Parameters,
                            out _rewrittenCommandText,
                            InputParameters,
                            ref batchCommands,
                            connector.UseConformingStrings);

                        if (batchCommands is not null)
                        {
                            // The command contained multiple statements, wrap in a batch
                            // TODO: Need to do something with CommandBehavior...
                            var batch = new NpgsqlBatch(conn, batchCommands);
                            return await batch.ExecuteReader(async, cancellationToken);
                        }

                        AutoPrepare(connector);

                        break;
                    }

                    try
                    {
                        // We cannot pass a token here, as we'll cancel a non-send query
                        // Also, we don't pass the cancellation token to StartUserAction, since that would make it scope to the entire action (command execution)
                        // whereas it should only be scoped to the Execute method.
                        connector.StartUserAction(ConnectorState.Executing, this, CancellationToken.None);

                        State = CommandState.InProgress;

                        if (Log.IsEnabled(NpgsqlLogLevel.Debug))
                            LogCommand();
                        NpgsqlEventSource.Log.CommandStart(CommandText);

                        // If a cancellation is in progress, wait for it to "complete" before proceeding (#615)
                        lock (connector.CancelLock)
                        {
                        }

                        // Note that we're in single statement execution. Unlike multiple (batched) statement execution, there is no risk of
                        // deadlock where PostgreSQL sends large results for the first statement, while we're sending large
                        // parameter data for the second (see NpgsqlBatch.ExecuteReader). We can therefore await the write operations as
                        // usual.
                        await SendExecute(connector, async, CancellationToken.None);
                        await connector.Flush(async, CancellationToken.None);
                    }
                    catch
                    {
                        conn.Connector?.EndUserAction();
                        throw;
                    }

                    // TODO: DRY the following with multiplexing, but be careful with the cancellation registration...
                    var reader = connector.DataReader;
                    reader.Init(this, behavior);
                    connector.CurrentReader = reader;
                    if (async)
                        await reader.NextResultAsync(cancellationToken);
                    else
                        reader.NextResult();

                    return reader;
                }
                else
                {
                    // The connection isn't bound to a connector - it's multiplexing time.

                    if (!async)
                    {
                        // The waiting on the ExecutionCompletion ManualResetValueTaskSource is necessarily
                        // asynchronous, so allowing sync would mean sync-over-async.
                        throw new NotSupportedException("Synchronous command execution is not supported when multiplexing is on");
                    }

                    ValidateParameters(conn.Pool!.MultiplexingTypeMapper!);
                    // TODO: Recycle
                    NpgsqlBatchCommandCollection? batchCommands = null;

                    var commandRewriter = new CommandRewriter();
                    commandRewriter.RewriteCommand(
                        CommandText,
                        CommandType,
                        Parameters,
                        out _rewrittenCommandText,
                        InputParameters,
                        ref batchCommands,
                        standardConformingStrings: true);

                    if (batchCommands is not null)
                        throw new NotSupportedException(@"Multi-statement commands aren't supported in multiplexing, use NpgsqlDbBatch");

                    State = CommandState.InProgress;

                    // TODO: Experiment: do we want to wait on *writing* here, or on *reading*?
                    // Previous behavior was to wait on reading, which throw the exception from ExecuteReader (and not from
                    // the first read). But waiting on writing would allow us to do sync writing and async reading.
                    ExecutionCompletion.Reset();
                    await conn.Pool!.MultiplexCommandWriter!.WriteAsync(this, cancellationToken);
                    connector = await new ValueTask<NpgsqlConnector>(ExecutionCompletion, ExecutionCompletion.Version);
                    // TODO: Overload of StartBindingScope?
                    conn.Connector = connector;
                    connector.Connection = conn;
                    conn.ConnectorBindingScope = ConnectorBindingScope.Reader;

                    var reader = connector.DataReader;
                    reader.Init(this, behavior);
                    connector.CurrentReader = reader;
                    await reader.NextResultAsync(cancellationToken);

                    return reader;
                }
            }
            catch (Exception e)
            {
                var reader = conn.Connector?.CurrentReader;
                if (!(e is NpgsqlOperationInProgressException) && reader != null)
                    await reader.Cleanup(async);

                State = CommandState.Idle;

                // Reader disposal contains logic for closing the connection if CommandBehavior.CloseConnection is
                // specified. However, close here as well in case of an error before the reader was even instantiated
                // (e.g. write I/O error)
                if ((behavior & CommandBehavior.CloseConnection) == CommandBehavior.CloseConnection)
                    conn.Close();
                throw;
            }
        }

        void AutoPrepare(NpgsqlConnector connector)
        {
            Debug.Assert(_rewrittenCommandText is not null);

            if (connector.Settings.MaxAutoPrepare == 0)
                return;

            // If this statement isn't prepared, see if it gets implicitly prepared.
            // Note that this may return null (not enough usages for automatic preparation).
            // TODO: Compare this with IsExplicitlyPrepared above. Not sure we need this distinction.
            if (!IsPrepared)
                PreparedStatement = connector.PreparedStatementManager.TryGetAutoPrepared(_rewrittenCommandText, InputParameters);

            if (PreparedStatement is PreparedStatement pStatement)
            {
                if (pStatement.State == PreparedState.NotPrepared)
                {
                    pStatement.State = PreparedState.BeingPrepared;
                    IsPreparing = true;
                }
                _connectorPreparedOn = connector;
                NpgsqlEventSource.Log.CommandStartPrepared();
            }
        }

        void IExecutable.AutoPrepare(NpgsqlConnector connector) => AutoPrepare(connector);

        #endregion

        #region Transactions

        /// <summary>
        /// DB transaction.
        /// </summary>
        protected override DbTransaction? DbTransaction
        {
            get => Transaction;
            set => Transaction = (NpgsqlTransaction?)value;
        }
        /// <summary>
        /// This property is ignored by Npgsql. PostgreSQL only supports a single transaction at a given time on
        /// a given connection, and all commands implicitly run inside the current transaction started via
        /// <see cref="NpgsqlConnection.BeginTransaction()"/>
        /// </summary>
        public new NpgsqlTransaction? Transaction { get; set; }

        #endregion Transactions

        #region Cancel

        /// <summary>
        /// Attempts to cancel the execution of an <see cref="NpgsqlCommand" />.
        /// </summary>
        /// <remarks>As per the specs, no exception will be thrown by this method in case of failure.</remarks>
        public override void Cancel()
        {
            if (State != CommandState.InProgress)
                return;

            var connection = Connection;
            if (connection is null)
                return;
            if (!connection.IsBound)
                throw new NotSupportedException("Cancellation not supported with multiplexing");

            connection.Connector?.PerformUserCancellation();
        }

        #endregion Cancel

        #region Dispose

        /// <summary>
        /// Releases the resources used by the <see cref="NpgsqlCommand"/>.
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (State == CommandState.Disposed)
                return;
            Transaction = null;
            _connection = null;
            State = CommandState.Disposed;
            base.Dispose(disposing);
        }

        #endregion

        #region Misc

        /// <summary>
        /// Fixes up the text/binary flag on result columns.
        /// Since Prepare() describes a statement rather than a portal, the resulting RowDescription
        /// will have text format on all result columns. Fix that up.
        /// </summary>
        /// <remarks>
        /// Note that UnknownResultTypeList only applies to the first query, while AllResultTypesAreUnknown applies
        /// to all of them.
        /// </remarks>
        internal void FixupRowDescription(RowDescriptionMessage rowDescription, bool isFirst)
        {
            for (var i = 0; i < rowDescription.NumFields; i++)
            {
                var field = rowDescription[i];
                field.FormatCode = (UnknownResultTypeList == null || !isFirst ? AllResultTypesAreUnknown : UnknownResultTypeList[i])
                    ? FormatCode.Text
                    : FormatCode.Binary;
                field.ResolveHandler();
            }
        }

        void LogCommand()
        {
            throw new NotImplementedException();
            // var connector = _connection!.Connector!;
            // var sb = new StringBuilder();
            // sb.AppendLine("Executing statement(s):");
            // foreach (var s in _statements)
            // {
            //     sb.Append("\t").AppendLine(s.SQL);
            //     var p = s.InputParameters;
            //     if (p.Count > 0 && (NpgsqlLogManager.IsParameterLoggingEnabled || connector.Settings.LogParameters))
            //     {
            //         for (var i = 0; i < p.Count; i++)
            //         {
            //             sb.Append("\t").Append("Parameters $").Append(i + 1).Append(":");
            //             switch (p[i].Value)
            //             {
            //             case IList list:
            //                 for (var j = 0; j < list.Count; j++)
            //                 {
            //                     sb.Append("\t#").Append(j).Append(": ").Append(Convert.ToString(list[j], CultureInfo.InvariantCulture));
            //                 }
            //                 break;
            //             case DBNull _:
            //             case null:
            //                 sb.Append("\t").Append(Convert.ToString("null", CultureInfo.InvariantCulture));
            //                 break;
            //             default:
            //                 sb.Append("\t").Append(Convert.ToString(p[i].Value, CultureInfo.InvariantCulture));
            //                 break;
            //             }
            //             sb.AppendLine();
            //         }
            //     }
            // }
            // Log.Debug(sb.ToString(), connector.Id);
            // connector.QueryLogStopWatch.Start();
        }

        /// <summary>
        /// Create a new command based on this one.
        /// </summary>
        /// <returns>A new NpgsqlCommand object.</returns>
        object ICloneable.Clone() => Clone();

        /// <summary>
        /// Create a new command based on this one.
        /// </summary>
        /// <returns>A new NpgsqlCommand object.</returns>
        public NpgsqlCommand Clone()
        {
            var clone = new NpgsqlCommand(CommandText, _connection, Transaction)
            {
                CommandTimeout = CommandTimeout, CommandType = CommandType, DesignTimeVisible = DesignTimeVisible, _allResultTypesAreUnknown = _allResultTypesAreUnknown, _unknownResultTypeList = _unknownResultTypeList, ObjectResultTypes = ObjectResultTypes
            };
            _parameters.CloneTo(clone._parameters);
            return clone;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        NpgsqlConnection CheckAndGetConnection()
        {
            if (State == CommandState.Disposed)
                throw new ObjectDisposedException(GetType().FullName);
            if (_connection == null)
                throw new InvalidOperationException("Connection property has not been initialized.");
            switch (_connection.FullState)
            {
            case ConnectionState.Open:
            case ConnectionState.Connecting:
            case ConnectionState.Open | ConnectionState.Executing:
            case ConnectionState.Open | ConnectionState.Fetching:
                return _connection;
            default:
                throw new InvalidOperationException("Connection is not open");
            }
        }

        /// <summary>
        /// This event is unsupported by Npgsql. Use <see cref="DbConnection.StateChange"/> instead.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public new event EventHandler? Disposed
        {
            add => throw new NotSupportedException("The Disposed event isn't supported by Npgsql. Use DbConnection.StateChange instead.");
            remove => throw new NotSupportedException("The Disposed event isn't supported by Npgsql. Use DbConnection.StateChange instead.");
        }

        event EventHandler? IComponent.Disposed
        {
            add => Disposed += value;
            remove => Disposed -= value;
        }

        #endregion
    }

    enum CommandState
    {
        Idle,
        InProgress,
        Disposed
    }
}
