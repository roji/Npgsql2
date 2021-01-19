using System;
using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Logging;
using Npgsql.Util;
using static Npgsql.Util.Statics;

#pragma warning disable 1591,RS0016

namespace Npgsql
{
    public sealed class NpgsqlBatch : DbBatch, IExecutable
    {
        readonly bool _performRewriting;
        int _state;
        NpgsqlConnection? _connection;

        /// <summary>
        /// If this command is (explicitly) prepared, references the connector on which the preparation happened.
        /// Used to detect when the connector was changed (i.e. connection open/close), meaning that the command
        /// is no longer prepared.
        /// </summary>
        NpgsqlConnector? _connectorPreparedOn;

        bool IsExplicitlyPrepared => _connectorPreparedOn != null;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(NpgsqlBatch));

        internal bool FlushOccurred { get; set; }

        protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

        public new NpgsqlBatchCommandCollection BatchCommands { get; }

        public NpgsqlBatch()
        {
            BatchCommands = new();
            _performRewriting = true;
        }

        public NpgsqlBatch(NpgsqlConnection? connection) : this()
            => Connection = connection;

        internal NpgsqlBatch(NpgsqlConnection connection, NpgsqlBatchCommandCollection batchCommandCollection)
        {
            Connection = connection;
            BatchCommands = batchCommandCollection;
            _performRewriting = false;
        }

        public override int Timeout { get; set; }

        protected override DbTransaction? DbTransaction { get; set; }

        ManualResetValueTaskSource<NpgsqlConnector> ExecutionCompletion { get; } = new();
        ManualResetValueTaskSource<NpgsqlConnector> IExecutable.ExecutionCompletion => ExecutionCompletion;

        public override void Cancel() => throw new NotImplementedException();

        #region State management

        /// <summary>
        /// The current state of the command
        /// </summary>
        internal CommandState State
        {
            private get { return (CommandState)_state; }
            set
            {
                var newState = (int)value;
                if (newState == _state)
                    return;
                Interlocked.Exchange(ref _state, newState);
            }
        }

        // TODO: Revisit
        CommandState IExecutable.State
        {
            set => State = value;
        }

        void ResetExplicitPreparation()
        {
            foreach (var s in BatchCommands)
                s.PreparedStatement = null;
            _connectorPreparedOn = null;
        }

        #endregion State management

        #region Connection management

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

        #endregion Connection management

        #region ExecuteScalar

        /// <summary>
        /// Executes the query, and returns the first column of the first row
        /// in the result set returned by the query. Extra columns or rows are ignored.
        /// </summary>
        /// <returns>The first column of the first row in the result set,
        /// or a null reference if the result set is empty.</returns>
        public override object? ExecuteScalar()
            => ExecuteScalar(false, CancellationToken.None).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous version of <see cref="ExecuteScalar()"/>
        /// </summary>
        /// <param name="cancellationToken">
        /// An optional token to cancel the asynchronous operation. The default value is <see cref="CancellationToken.None"/>.
        /// </param>
        /// <returns>A task representing the asynchronous operation, with the first column of the
        /// first row in the result set, or a null reference if the result set is empty.</returns>
        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return ExecuteScalar(true, cancellationToken).AsTask();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        async ValueTask<object?> ExecuteScalar(bool async, CancellationToken cancellationToken)
        {
            // TODO: Think about the command behavior.
            // Unlike with NpgsqlCommand, users are free to populate whatever command behavior they want on each
            // BatchCommand, so we don't necessarily have to apply anything ourselves.
            using var reader = await ExecuteReader(async, cancellationToken);
            return reader.Read() && reader.FieldCount != 0 ? reader.GetValue(0) : null;
        }

        #endregion

        #region ExecuteNonQuery

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
        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return ExecuteNonQuery(true, cancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        async Task<int> ExecuteNonQuery(bool async, CancellationToken cancellationToken)
        {
            using var reader = await ExecuteReader(async, cancellationToken);
            while (async ? await reader.NextResultAsync(cancellationToken) : reader.NextResult()) ;

            return reader.RecordsAffected;
        }

        #endregion ExecuteNonQuery

        #region ExecuteReader

        protected override DbDataReader ExecuteDbDataReader()
            => ExecuteReader();

        public new NpgsqlDataReader ExecuteReader()
            => ExecuteReader(async: false, CancellationToken.None).GetAwaiter().GetResult();

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CancellationToken cancellationToken)
            => await ExecuteReaderAsync(cancellationToken);

        public new Task<NpgsqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return ExecuteReader(async: true, cancellationToken).AsTask();
        }

        internal async ValueTask<NpgsqlDataReader> ExecuteReader(bool async, CancellationToken cancellationToken)
        {
            var conn = CheckAndGetConnection();

            try
            {
                if (conn.TryGetBoundConnector(out var connector))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    // We cannot pass a token here, as we'll cancel a non-send query
                    // Also, we don't pass the cancellation token to StartUserAction, since that would make it scope to the entire action (command execution)
                    // whereas it should only be scoped to the Execute method.
                    // TODO: Pass `this` in
                    connector.StartUserAction(ConnectorState.Executing, null, CancellationToken.None);

                    Task? sendTask;

                    try
                    {
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

                            foreach (var batchCommand in BatchCommands)
                            {
                                batchCommand.ValidateParameters(connector.TypeMapper);
                            }

                            NpgsqlEventSource.Log.CommandStartPrepared();
                            break;

                        case false:
                            foreach (var batchCommand in BatchCommands)
                            {
                                batchCommand.ValidateParameters(connector.TypeMapper);
                                if (_performRewriting)
                                {
                                    batchCommand.RewriteCommand(connector.UseConformingStrings);
                                    Debug.Assert(batchCommand.RewrittenCommandText is not null);
                                }
                            }
                            AutoPrepare(connector);
                            break;
                        }

                        State = CommandState.InProgress;

                        if (Log.IsEnabled(NpgsqlLogLevel.Debug))
                            LogBatch();
                        // NpgsqlEventSource.Log.CommandStart(CommandText);

                        // If a cancellation is in progress, wait for it to "complete" before proceeding (#615)
                        lock (connector.CancelLock)
                        {
                        }

                        // We do not wait for the entire send to complete before proceeding to reading -
                        // the sending continues in parallel with the user's reading. Waiting for the
                        // entire send to complete would trigger a deadlock for multi-statement commands,
                        // where PostgreSQL sends large results for the first statement, while we're sending large
                        // parameter data for the second. See #641.
                        // Instead, all sends for non-first statements and for non-first buffers are performed
                        // asynchronously (even if the user requested sync), in a special synchronization context
                        // to prevents a dependency on the thread pool (which would also trigger deadlocks).
                        // The WriteBuffer notifies this command when the first buffer flush occurs, so that the
                        // send functions can switch to the special async mode when needed.
                        sendTask = NonMultiplexingWriteWrapper(connector, async, CancellationToken.None);

                        // The following is a hack. It raises an exception if one was thrown in the first phases
                        // of the send (i.e. in parts of the send that executed synchronously). Exceptions may
                        // still happen later and aren't properly handled. See #1323.
                        if (sendTask.IsFaulted)
                            sendTask.GetAwaiter().GetResult();
                    }
                    catch
                    {
                        conn.Connector?.EndUserAction();
                        throw;
                    }

                    // TODO: DRY the following with multiplexing, but be careful with the cancellation registration...
                    var reader = connector.DataReader;
                    // TODO: Implement per-command CommandBehavior
                    reader.Init(this, CommandBehavior.Default, sendTask);
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

                    foreach (var batchCommand in BatchCommands)
                    {
                        batchCommand.ValidateParameters(conn.Pool!.MultiplexingTypeMapper!);
                        batchCommand.RewriteCommand(standardConformingStrings: true);
                        Debug.Assert(batchCommand.RewrittenCommandText is not null);
                    }

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
                    // TODO: Implement per-command CommandBehavior
                    reader.Init(this, CommandBehavior.Default);
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
                // TODO: Validate that all batch commands have the same value here
                if (BatchCommands.Count > 0 && BatchCommands[0].CommandBehavior.HasFlag(CommandBehavior.CloseConnection))
                    await conn.Close(async, CancellationToken.None);
                throw;
            }

            async Task NonMultiplexingWriteWrapper(NpgsqlConnector connector, bool async, CancellationToken cancellationToken2)
            {
                BeginSend(connector);
                await SendExecute(connector, async, cancellationToken2);
                await connector.Flush(async, cancellationToken2);
                CleanupSend();
            }
        }

        void AutoPrepare(NpgsqlConnector connector)
        {
            if (connector.Settings.MaxAutoPrepare == 0)
                return;

            var numPrepared = 0;

            foreach (var batchCommand in BatchCommands)
            {
                Debug.Assert(batchCommand.RewrittenCommandText is not null);

                // If this statement isn't prepared, see if it gets implicitly prepared.
                // Note that this may return null (not enough usages for automatic preparation).
                if (!batchCommand.IsPrepared)
                {
                    batchCommand.PreparedStatement = connector.PreparedStatementManager.TryGetAutoPrepared(
                        batchCommand.RewrittenCommandText, batchCommand.InputParameters);
                }

                if (batchCommand.PreparedStatement is PreparedStatement pStatement)
                {
                    numPrepared++;
                    if (pStatement.State == PreparedState.NotPrepared)
                    {
                        pStatement.State = PreparedState.BeingPrepared;
                        batchCommand.IsPreparing = true;
                    }
                }
            }

            if (numPrepared > 0)
            {
                _connectorPreparedOn = connector;
                if (numPrepared == BatchCommands.Count)
                    NpgsqlEventSource.Log.CommandStartPrepared();
            }
        }

        void IExecutable.AutoPrepare(NpgsqlConnector connector) => AutoPrepare(connector);

        async Task SendExecute(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default)
        {
            // return (_behavior & CommandBehavior.SchemaOnly) == 0
            //     ? WriteExecute(connector, async)
            //     : WriteExecuteSchemaOnly(connector, async);

            // TODO: Support interleaving SchemaOnly with non-SchemaOnly...?
            // Maybe just not support SchemaOnly with batches, at least for now?

            for (var i = 0; i < BatchCommands.Count; i++)
            {
                // The following is only for deadlock avoidance when doing sync I/O (so never in multiplexing)
                ForceAsyncIfNecessary(ref async, i);

                var batchCommand = BatchCommands[i];
                var pStatement = batchCommand.PreparedStatement;
                Debug.Assert(!batchCommand.CommandBehavior.HasFlag(CommandBehavior.SchemaOnly)); // At least for now

                if (pStatement == null || batchCommand.IsPreparing)
                {
                    // The statement should either execute unprepared, or is being auto-prepared.
                    // Send Parse, Bind, Describe

                    // We may have a prepared statement that replaces an existing statement - close the latter first.
                    if (pStatement?.StatementBeingReplaced != null)
                        await connector.WriteClose(StatementOrPortal.Statement, pStatement.StatementBeingReplaced.Name!, async, cancellationToken);

                    Debug.Assert(batchCommand.RewrittenCommandText is not null, "RewrittenCommandText is null in Write");
                    await connector.WriteParse(batchCommand.RewrittenCommandText, batchCommand.StatementName, batchCommand.InputParameters, async, cancellationToken);

                    await connector.WriteBind(
                        batchCommand.InputParameters, string.Empty, batchCommand.StatementName, batchCommand.AllResultTypesAreUnknown,
                        batchCommand.UnknownResultTypeList,
                        async, cancellationToken);

                    await connector.WriteDescribe(StatementOrPortal.Portal, string.Empty, async, cancellationToken);
                }
                else
                {
                    // The statement is already prepared, only a Bind is needed
                    await connector.WriteBind(
                        batchCommand.InputParameters, string.Empty, batchCommand.StatementName, batchCommand.AllResultTypesAreUnknown,
                        batchCommand.UnknownResultTypeList,
                        async, cancellationToken);
                }

                await connector.WriteExecute(0, async, cancellationToken);

                if (pStatement != null)
                    pStatement.LastUsed = DateTime.UtcNow;
            }

            await connector.WriteSync(async, cancellationToken);

            // async Task WriteExecuteSchemaOnly(NpgsqlConnector connector, bool async)
            // {
            //     var wroteSomething = false;
            //     for (var i = 0; i < _statements.Count; i++)
            //     {
            //         async = ForceAsyncIfNecessary(async, i);
            //
            //         var statement = _statements[i];
            //
            //         if (statement.PreparedStatement?.State == PreparedState.Prepared)
            //             continue;   // Prepared, we already have the RowDescription
            //         Debug.Assert(statement.PreparedStatement == null);
            //
            //         await connector.WriteParse(statement.SQL, string.Empty, statement.InputParameters, async, cancellationToken);
            //         await connector.WriteDescribe(StatementOrPortal.Statement, statement.StatementName, async, cancellationToken);
            //         wroteSomething = true;
            //     }
            //
            //     if (wroteSomething)
            //         await connector.WriteSync(async, cancellationToken);
            // }
        }

        Task IExecutable.SendExecute(NpgsqlConnector connector, bool async, CancellationToken cancellationToken)
            => SendExecute(connector, async, cancellationToken);

        #endregion ExecuteReader

        #region Prepare

        public override void Prepare() => Prepare(async: false).GetAwaiter().GetResult();

        public override Task PrepareAsync(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return Prepare(true, cancellationToken);
        }

        internal Task Prepare(bool async, CancellationToken cancellationToken = default)
        {
            var connection = CheckAndGetConnection();
            if (connection.Settings.Multiplexing)
                throw new NotSupportedException("Explicit preparation not supported with multiplexing");
            var connector = connection.Connector!;

            if (Log.IsEnabled(NpgsqlLogLevel.Debug))
                Log.Debug("Preparing: " + string.Join("; ", BatchCommands.Select(c => c.CommandText)), connector.Id);

            var needToPrepare = false;
            foreach (var batchCommand in BatchCommands)
            {
                if (batchCommand.IsPrepared)
                    continue;

                if (_performRewriting)
                    batchCommand.RewriteCommand(connector.UseConformingStrings);
                Debug.Assert(batchCommand.RewrittenCommandText is not null);

                foreach (NpgsqlParameter parameter in batchCommand.InputParameters)
                    parameter.Bind(connector.TypeMapper);

                batchCommand.PreparedStatement = connector.PreparedStatementManager.GetOrAddExplicit(
                    batchCommand.RewrittenCommandText, batchCommand.InputParameters);
                if (batchCommand.PreparedStatement?.State == PreparedState.NotPrepared)
                {
                    batchCommand.PreparedStatement.State = PreparedState.BeingPrepared;
                    batchCommand.IsPreparing = true;
                    needToPrepare = true;
                }
            }

            _connectorPreparedOn = connector;

            // It's possible the command was already prepared, or that persistent prepared statements were found for
            // all statements. Nothing to do here, move along.
            return needToPrepare
                ? PrepareLong()
                : Task.CompletedTask;

            async Task PrepareLong()
            {
                try
                {
                    using (connector.StartUserAction(cancellationToken))
                    {
                        var sendTask = SendPrepare(connector, async, cancellationToken);
                        if (sendTask.IsFaulted)
                            sendTask.GetAwaiter().GetResult();

                        // Loop over statements, skipping those that are already prepared (because they were persisted)
                        var isFirst = true;
                        for (var i = 0; i < BatchCommands.Count; i++)
                        {
                            var batchCommand = BatchCommands[i];
                            if (!batchCommand.IsPreparing)
                                continue;

                            var pStatement = batchCommand.PreparedStatement!;

                            if (pStatement.StatementBeingReplaced != null)
                            {
                                Expect<CloseCompletedMessage>(await connector.ReadMessage(async), connector);
                                pStatement.StatementBeingReplaced.CompleteUnprepare();
                                pStatement.StatementBeingReplaced = null;
                            }

                            Expect<ParseCompleteMessage>(await connector.ReadMessage(async), connector);
                            Expect<ParameterDescriptionMessage>(await connector.ReadMessage(async), connector);
                            var msg = await connector.ReadMessage(async);
                            switch (msg.Code)
                            {
                            case BackendMessageCode.RowDescription:
                                // Clone the RowDescription for use with the prepared statement (the one we have is reused
                                // by the connection)
                                var description = ((RowDescriptionMessage)msg).Clone();
                                batchCommand.FixupRowDescription(description, isFirst);
                                batchCommand.Description = description;
                                break;
                            case BackendMessageCode.NoData:
                                batchCommand.Description = null;
                                break;
                            default:
                                throw connector.UnexpectedMessageReceived(msg.Code);
                            }

                            batchCommand.IsPreparing = false;
                            pStatement.CompletePrepare();
                            isFirst = false;
                        }

                        Expect<ReadyForQueryMessage>(await connector.ReadMessage(async), connector);

                        if (async)
                            await sendTask;
                        else
                            sendTask.GetAwaiter().GetResult();
                    }
                }
                catch
                {
                    // The statements weren't prepared successfully, update the bookkeeping for them
                    foreach (var statement in BatchCommands)
                    {
                        if (statement.IsPreparing)
                        {
                            statement.IsPreparing = false;
                            statement.PreparedStatement!.CompleteUnprepare();
                        }
                    }

                    throw;
                }
            }
        }

        async Task SendPrepare(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default)
        {
            BeginSend(connector);

            for (var i = 0; i < BatchCommands.Count; i++)
            {
                ForceAsyncIfNecessary(ref async, i);

                var batchCommand = BatchCommands[i];
                Debug.Assert(batchCommand.RewrittenCommandText is not null);

                var pStatement = batchCommand.PreparedStatement;

                // A statement may be already prepared, already in preparation (i.e. same statement twice
                // in the same command), or we can't prepare (overloaded SQL)
                if (!batchCommand.IsPreparing)
                    continue;

                // We may have a prepared statement that replaces an existing statement - close the latter first.
                var statementToClose = pStatement!.StatementBeingReplaced;
                if (statementToClose != null)
                    await connector.WriteClose(StatementOrPortal.Statement, statementToClose.Name!, async, cancellationToken);

                await connector.WriteParse(batchCommand.RewrittenCommandText, pStatement.Name!, batchCommand.InputParameters, async, cancellationToken);
                await connector.WriteDescribe(StatementOrPortal.Statement, pStatement.Name!, async, cancellationToken);
            }

            await connector.WriteSync(async, cancellationToken);
            await connector.Flush(async, cancellationToken);

            CleanupSend();
        }

        #endregion

        Type[]? IExecutable.ObjectResultTypes => null;

        void BeginSend(NpgsqlConnector connector)
        {
            connector.WriteBuffer.Timeout = TimeSpan.FromSeconds(Timeout);
            connector.WriteBuffer.CurrentBatch = this;
            FlushOccurred = false;
        }

        void CleanupSend()
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (SynchronizationContext.Current != null)  // Check first because SetSynchronizationContext allocates
                SynchronizationContext.SetSynchronizationContext(null);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ForceAsyncIfNecessary(ref bool async, int numberOfStatementInBatch)
        {
            if (!async && FlushOccurred && numberOfStatementInBatch > 0)
            {
                // We're synchronously sending the non-first statement in a batch and a flush
                // has already occured. Switch to async. See long comment in Execute() above.
                async = true;
                SynchronizationContext.SetSynchronizationContext(SingleThreadSynchronizationContext.Instance);
            }
        }

        void LogBatch()
        {
            var connector = _connection!.Connector!;
            var sb = new StringBuilder();
            sb.AppendLine("Executing batch commands:");
            foreach (var s in BatchCommands)
            {
                sb.Append("\t").AppendLine(s.CommandText);
                var p = s.Parameters;
                if (p.Count > 0 && (NpgsqlLogManager.IsParameterLoggingEnabled || connector.Settings.LogParameters))
                {
                    for (var i = 0; i < p.Count; i++)
                    {
                        sb.Append("\t").Append("Parameters $").Append(i + 1).Append(":");
                        switch (p[i].Value)
                        {
                        case IList list:
                            for (var j = 0; j < list.Count; j++)
                            {
                                sb.Append("\t#").Append(j).Append(": ").Append(Convert.ToString(list[j], CultureInfo.InvariantCulture));
                            }
                            break;
                        case DBNull _:
                        case null:
                            sb.Append("\t").Append(Convert.ToString("null", CultureInfo.InvariantCulture));
                            break;
                        default:
                            sb.Append("\t").Append(Convert.ToString(p[i].Value, CultureInfo.InvariantCulture));
                            break;
                        }
                        sb.AppendLine();
                    }
                }
            }

            Log.Debug(sb.ToString(), connector.Id);
            connector.QueryLogStopWatch.Start();
        }
    }
}
