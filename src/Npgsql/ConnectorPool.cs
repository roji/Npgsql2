using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Transactions;
using Npgsql.Logging;
using Npgsql.Util;

namespace Npgsql
{
    sealed class ConnectorPool
    {
        #region Fields and properties

        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(ConnectorPool));

        internal long NumCommandsSent;
        internal long NumFlushes;

        internal NpgsqlConnectionStringBuilder Settings { get; }

        /// <summary>
        /// Contains the connection string returned to the user from <see cref="NpgsqlConnection.ConnectionString"/>
        /// after the connection has been opened. Does not contain the password unless Persist Security Info=true.
        /// </summary>
        internal string UserFacingConnectionString { get; }

        string _connectionString { get; }

        readonly int _max;
        readonly int _min;
        readonly bool _autoPrepare;
        volatile int _numConnectors;
        volatile int _idle;

        /// <summary>
        /// When multiplexing is enabled, determines the maximum amount of time to wait for further
        /// commands before flushing to the network. In ticks (100ns), 0 disables waiting.
        /// This is in 100ns ticks, not <see cref="Stopwatch"/> ticks whose meaning vary across platforms.
        /// </summary>
        readonly long _writeCoalescingDelayTicks;

        /// <summary>
        /// When multiplexing is enabled, determines the maximum number of outgoing bytes to buffer before
        /// flushing to the network.
        /// </summary>
        readonly int _writeCoalescingBufferThresholdBytes;

        readonly ChannelReader<NpgsqlConnector> _idleConnectorReader;
        internal ChannelWriter<NpgsqlConnector> IdleConnectorWriter { get; }

        readonly ChannelReader<NpgsqlCommand>? _multiplexCommandReader;
        internal ChannelWriter<NpgsqlCommand>? MultiplexCommandWriter { get; }

        /// <summary>
        /// Incremented every time this pool is cleared via <see cref="NpgsqlConnection.ClearPool"/> or
        /// <see cref="NpgsqlConnection.ClearAllPools"/>. Allows us to identify connections which were
        /// created before the clear.
        /// </summary>
        volatile int _clearCounter;

        static readonly TimerCallback PruningTimerCallback = PruneIdleConnectors;
        readonly Timer _pruningTimer;
        readonly TimeSpan _pruningSamplingInterval;
        readonly int _pruningSampleSize;
        readonly int[] _pruningSamples;
        readonly int _pruningMedianIndex;
        volatile bool _pruningTimerEnabled;
        int _pruningSampleIndex;

        long _ticksFlushed;
        long _bytesFlushed;

        // Note that while the dictionary is protected by locking, we assume that the lists it contains don't need to be
        // (i.e. access to connectors of a specific transaction won't be concurrent)
        readonly Dictionary<Transaction, List<NpgsqlConnector>> _pendingEnlistedConnectors
            = new Dictionary<Transaction, List<NpgsqlConnector>>();

        #endregion

        internal (int Total, int Idle, int Busy) Statistics
        {
            get
            {
                var numConnectors = _numConnectors;
                var idle = _idle;
                return (numConnectors, idle, numConnectors - idle);
            }
        }

        internal ConnectorPool(NpgsqlConnectionStringBuilder settings, string connString)
        {
            if (settings.MaxPoolSize < settings.MinPoolSize)
                throw new ArgumentException($"Connection can't have MaxPoolSize {settings.MaxPoolSize} under MinPoolSize {settings.MinPoolSize}");

            // We enforce Max Pool Size anyway, so no need to to create a bounded channel (which is less efficient)
            var idleChannel = Channel.CreateUnbounded<NpgsqlConnector>();
            _idleConnectorReader = idleChannel.Reader;
            IdleConnectorWriter = idleChannel.Writer;

            _max = settings.MaxPoolSize;
            _min = settings.MinPoolSize;

            UserFacingConnectionString = settings.PersistSecurityInfo
                ? connString
                : settings.ToStringWithoutPassword();

            Settings = settings;

            if (settings.ConnectionPruningInterval == 0)
                throw new ArgumentException("ConnectionPruningInterval can't be 0.");
            var connectionIdleLifetime = TimeSpan.FromSeconds(settings.ConnectionIdleLifetime);
            var pruningSamplingInterval = TimeSpan.FromSeconds(settings.ConnectionPruningInterval);
            if (connectionIdleLifetime < pruningSamplingInterval)
                throw new ArgumentException($"Connection can't have ConnectionIdleLifetime {connectionIdleLifetime} under ConnectionPruningInterval {_pruningSamplingInterval}");

            _pruningTimer = new Timer(PruningTimerCallback, this, Timeout.Infinite, Timeout.Infinite);
            _pruningSampleSize = DivideRoundingUp(connectionIdleLifetime.Seconds, pruningSamplingInterval.Seconds);
            _pruningMedianIndex = DivideRoundingUp(_pruningSampleSize, 2) - 1; // - 1 to go from length to index
            _pruningSamplingInterval = pruningSamplingInterval;
            _pruningSamples = new int[_pruningSampleSize];
            _pruningTimerEnabled = false;

            _max = settings.MaxPoolSize;
            _min = settings.MinPoolSize;
            _autoPrepare = settings.MaxAutoPrepare > 0;

            _connectionString = connString;

            UserFacingConnectionString = settings.PersistSecurityInfo
                ? connString
                : settings.ToStringWithoutPassword();

            // TODO: Validate multiplexing options are set only when Multiplexing is on

            if (Settings.Multiplexing)
            {
                // The connection string contains the delay in microseconds, but we need it in Stopwatch ticks,
                // whose meaning varies by platform. Do the translation.
                _writeCoalescingDelayTicks = Settings.WriteCoalescingDelayUs * (Stopwatch.Frequency / 1_000_000L);
                _writeCoalescingBufferThresholdBytes = Settings.WriteCoalescingBufferThresholdBytes;
                // TODO: Add number of commands written threshold

                // TODO: Make this bounded
                var multiplexCommandChannel = Channel.CreateUnbounded<NpgsqlCommand>(
                    new UnboundedChannelOptions { SingleReader = true });
                _multiplexCommandReader = multiplexCommandChannel.Reader;
                MultiplexCommandWriter = multiplexCommandChannel.Writer;

                MultiplexingWriteLoop();
            }
        }

        internal ValueTask<NpgsqlConnector> Rent(NpgsqlConnection conn, NpgsqlTimeout timeout, bool async,
            CancellationToken cancellationToken)
        {
            Counters.SoftConnectsPerSecond.Increment();

            if (TryGetIdleConnector(out var connector))
            {
                connector.Connection = conn;
                return new ValueTask<NpgsqlConnector>(connector);
            }

            return RentAsync();

            async ValueTask<NpgsqlConnector> RentAsync()
            {
                // TODO: If we're synchronous, use SingleThreadSynchronizationContext to not schedule completions
                // on the thread pool (standard sync-over-async TP pseudo-deadlock)
                if (await OpenNewConnector(conn, timeout, async, cancellationToken) is NpgsqlConnector newConnector)
                    return newConnector;

                // We're at max capacity. Asynchronously wait on the idle channel.

                // TODO: Potential issue: we only check to create new connections once above. In theory we could have
                // many attempts waiting on the idle channel forever, since all connections were broken by some network
                // event. Pretty sure this issue exists in the old lock-free implementation too, think about it (it would
                // be enough to retry the physical creation above).

                var timeoutSource = new CancellationTokenSource(timeout.TimeLeft);
                var timeoutToken = timeoutSource.Token;
                using var _ = cancellationToken.Register(cts => ((CancellationTokenSource)cts!).Cancel(), timeoutSource);
                try
                {
                    while (await _idleConnectorReader.WaitToReadAsync(timeoutToken))
                    {
                        if (TryGetIdleConnector(out connector))
                        {
                            connector.Connection = conn;
                            return connector;
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    throw new NpgsqlException(
                        $"The connection pool has been exhausted, either raise MaxPoolSize (currently {_max}) " +
                        $"or Timeout (currently {Settings.Timeout} seconds)");
                }

                // TODO: The channel has been completed, the pool is being disposed. Does this actually occur?
                throw new NpgsqlException("The connection pool has been shut down.");
            }

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryGetIdleConnector([NotNullWhen(true)] out NpgsqlConnector connector)
        {
            while (_idleConnectorReader.TryRead(out connector))
            {
                Counters.NumberOfFreeConnections.Decrement();

                // An connector could be broken because of a keepalive that occurred while it was
                // idling in the pool
                // TODO: Consider removing the pool from the keepalive code. The following branch is simply irrelevant
                // if keepalive isn't turned on.
                if (connector.IsBroken)
                {
                    CloseConnector(connector);
                    continue;
                }

                Counters.NumberOfActiveConnections.Increment();
                Interlocked.Decrement(ref _idle);
                return true;
            }

            return false;
        }

        async ValueTask<NpgsqlConnector?> OpenNewConnector(
            NpgsqlConnection conn, NpgsqlTimeout timeout, bool async, CancellationToken cancellationToken)
        {
            // As long as we're under max capacity, attempt to increase the connector count and open a new connection.
            for (var numConnectors = _numConnectors; numConnectors < _max; numConnectors = _numConnectors)
            {
                // Note that we purposefully don't use SpinWait for this: https://github.com/dotnet/coreclr/pull/21437
                if (Interlocked.CompareExchange(ref _numConnectors, numConnectors + 1, numConnectors) !=
                    numConnectors)
                    continue;

                try
                {
                    // We've managed to increase the open counter, open a physical connections.
                    var connector = new NpgsqlConnector(conn) { ClearCounter = _clearCounter };
                    await connector.Open(timeout, async, cancellationToken);

                    // Only start pruning if it was this thread that incremented open count past _min.
                    if (numConnectors == _min)
                        EnablePruning();
                    Counters.NumberOfPooledConnections.Increment();
                    Counters.NumberOfActiveConnections.Increment();

                    return connector;
                }
                catch
                {
                    // Physical open failed, decrement the open and busy counter back down.
                    conn.Connector = null;
                    Interlocked.Decrement(ref _numConnectors);
                    throw;
                }
            }

            return null;
        }

        internal void Return(NpgsqlConnector connector)
        {
            Counters.SoftDisconnectsPerSecond.Increment();
            Counters.NumberOfActiveConnections.Decrement();

            // If Clear/ClearAll has been been called since this connector was first opened,
            // throw it away. The same if it's broken (in which case CloseConnector is only
            // used to update state/perf counter).
            if (connector.ClearCounter < _clearCounter || connector.IsBroken)
            {
                CloseConnector(connector);
                return;
            }

            connector.Reset();

            // Order is important since we have synchronous completions on the channel.
            Interlocked.Increment(ref _idle);
            var written = IdleConnectorWriter.TryWrite(connector);
            Debug.Assert(written);
        }

        internal void Clear()
        {
            Interlocked.Increment(ref _clearCounter);
            while (TryGetIdleConnector(out var connector))
                CloseConnector(connector);
        }

        void CloseConnector(NpgsqlConnector connector)
        {
            try
            {
                connector.Close();
            }
            catch (Exception e)
            {
                Log.Warn("Exception while closing outdated connector", e, connector.Id);
            }

            Counters.NumberOfPooledConnections.Decrement();

            var numConnectors = Interlocked.Decrement(ref _numConnectors);
            Debug.Assert(numConnectors >= 0);
            // Only turn off the timer one time, when it was this Close that brought Open back to _min.
            if (numConnectors == _min)
                DisablePruning();
        }

        #region Multiplexing

        async void MultiplexingWriteLoop()
        {
            Debug.Assert(_multiplexCommandReader != null);

            // TODO: Writing I/O here is currently async-only. Experiment with both sync and async (based on user
            // preference, ExecuteReader vs. ExecuteReaderAsync).
            while (await _multiplexCommandReader.WaitToReadAsync())
            {
                // First read one command. In case there's failure getting a connection below, that command
                // will be failed.
                // TODO: If we're SingleReader, we don't need this check
                if (!_multiplexCommandReader.TryRead(out var command))
                    continue;

                NpgsqlConnector connector;
                try
                {
                    if (!TryGetIdleConnector(out NpgsqlConnector? newConnector))
                    {
                        // TODO: Need the authentication callbacks...

                        // TODO: Yes, the following is super ugly, look again at making NpgsqlConnector autonomous
                        // without NpgsqlConnection
                        var tempConn = new NpgsqlConnection(_connectionString);
                        newConnector = await OpenNewConnector(
                            tempConn,
                            new NpgsqlTimeout(TimeSpan.FromSeconds(Settings.Timeout)),
                            async: true,
                            CancellationToken.None);

                        if (newConnector == null)
                        {
                            // There were no idle connectors and we're at max capacity, so we can't open a new one.
                            // Execute the command on one
                            // TODO: Implement this, for now we fail
                            command.ExecutionCompletion.SetException(
                                new NotImplementedException("No idle connectors left and we're at max capacity"));
                            continue;
                        }

                        newConnector.Connection = null;
                    }

                    connector = newConnector;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception opening a connection" + ex);
                    Log.Error("Exception opening a connection", ex);
                    command.ExecutionCompletion.SetException(ex);
                    continue;
                }

                CrappyLog.Write($"CMD?????|CON{connector.Id:00000}: Connector being prepared");

                using var cts = new CancellationTokenSource(TimeSpan.FromTicks(_writeCoalescingDelayTicks));
                var sw = Stopwatch.StartNew(); // TODO: Remove?

                var numCommandsWritten = 0;

                // Read queued commands and write them to the connector's buffer, for as long as we're
                // under our write threshold and timer delay.
                // Note we already have one command we read above.
                try
                {
                    while (true)
                    {
                        do
                        {
                            WriteCommand(connector, command);
                            numCommandsWritten++;

                            if (connector.WriteBuffer.WritePosition >= _writeCoalescingBufferThresholdBytes)
                                goto DoneWriting;
                        } while (_multiplexCommandReader.TryRead(out command));

                        do
                        {
                            await _multiplexCommandReader.WaitToReadAsync(cts.Token);
                            // TODO: If we're SingleReader, we don't need this check
                        } while (!_multiplexCommandReader.TryRead(out command));
                    }
                    DoneWriting:

                    _ticksFlushed += sw.ElapsedTicks;
                    _bytesFlushed += connector.WriteBuffer.WritePosition;

                    CrappyLog.Write($"CMD?????|CON{connector.Id:00000}: flushing connector ({numCommandsWritten} commands)");

#pragma warning disable 4014
                    // Purposefully don't wait for flushing to complete. That can happen in parallel to us treating
                    // more commands on a different connector.
                    // TODO: be careful to not continue sending more commands on the connector before flushing has completed
                    connector.Flush(true);
#pragma warning restore 4014

                    // TODO: Remove these crappy statistics and do something nice with perf counters
                    Interlocked.Add(ref NumCommandsSent, numCommandsWritten);
                    var numFlushes = Interlocked.Increment(ref NumFlushes);
                    if (numFlushes % 100000 == 0)
                    {
                        Console.WriteLine(
                            $"Commands: Average commands per flush: {(double)NumCommandsSent / NumFlushes} " +
                            $"({NumCommandsSent}/{NumFlushes})");
                        Console.WriteLine($"Total physical connections: {_numConnectors}");
                        Console.WriteLine($"Average flush time: {_ticksFlushed / NumFlushes}");
                        Console.WriteLine($"Average write buffer position: {_bytesFlushed / NumFlushes}");
                    }
                }
                catch (OperationCanceledException)
                {
                    // Timeout fired, we're done writing
                }
                catch (Exception ex)
                {
                    // All commands already passed validation before being enqueued, so we assume that any
                    // error here is an unrecoverable network issue during Flush, which means we
                    // should be broken.
                    Debug.Assert(connector.State == ConnectorState.Broken,
                        "Exception thrown while writing commands but connector isn't broken");

                    // The connector's read loop will also receive an exception as the connector was broken,
                    // and will drain the CommandsInFlight queue, failing all commands.
                    Log.Error("Exception while writing commands", ex, connector.Id);
                }
            }

            void WriteCommand(NpgsqlConnector connector, NpgsqlCommand command)
            {
                command.Connection!.Connector = connector;

                // TODO: Need to flow the behavior (SchemaOnly support etc.), cancellation token, async-ness (?)...
                // TODO: Stop at some point even if there are pending commands (otherwise we could continue
                // serializing forever). Define a threshold for filling the buffer beyond which we flush.
                // TODO: Error handling here... everything written to the buffer must be errored,
                // we could dispatch later commands to another connection in the pool. We could even retry
                // ones that have already been tried - this means that we need to be able to write a command
                // twice.

                if (_autoPrepare)
                {
                    var numPrepared = 0;
                    foreach (var statement in command._statements)
                    {
                        // If this statement isn't prepared, see if it gets implicitly prepared.
                        // Note that this may return null (not enough usages for automatic preparation).
                        if (!statement.IsPrepared)
                            statement.PreparedStatement =
                                connector.PreparedStatementManager.TryGetAutoPrepared(statement);
                        if (statement.PreparedStatement != null)
                            numPrepared++;
                    }
                }

                connector.CommandsInFlight.Enqueue(command);

                // Purposefully don't wait for I/O to complete
                // TODO: Different path for synchronous completion here? In the common/hot case this will return
                // synchronously, is it optimizing for that? That could mean we flush immediately (much like
                // threshold exceeded).
                // For the TE prototype this never occurs, so don't care.
                var task = command.WriteExecute(connector, async: true);
                if (!task.IsCompletedSuccessfully)
                    throw new Exception("When writing Execute to connector, task is in state" + task.Status);
                CrappyLog.Write($"CMD{command.Id:00000}|CON{connector.Id:00000}: command written to connector");
            }
        }

        #endregion

        #region Pending Enlisted Connections

        internal void AddPendingEnlistedConnector(NpgsqlConnector connector, Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    list = _pendingEnlistedConnectors[transaction] = new List<NpgsqlConnector>();
                list.Add(connector);
            }
        }

        internal void TryRemovePendingEnlistedConnector(NpgsqlConnector connector, Transaction transaction)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                    return;
                list.Remove(connector);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
            }
        }

        internal bool TryRentEnlistedPending(
            NpgsqlConnection connection,
            Transaction transaction,
            [NotNullWhen(true)] out NpgsqlConnector? connector)
        {
            lock (_pendingEnlistedConnectors)
            {
                if (!_pendingEnlistedConnectors.TryGetValue(transaction, out var list))
                {
                    connector = null;
                    return false;
                }
                connector = list[list.Count - 1];
                list.RemoveAt(list.Count - 1);
                if (list.Count == 0)
                    _pendingEnlistedConnectors.Remove(transaction);
                connector.Connection = connection;
                return true;
            }
        }

        #endregion

        #region Pruning

        // Manual reactivation of timer happens in callback
        void EnablePruning()
        {
            lock (_pruningTimer)
            {
                _pruningTimerEnabled = true;
                _pruningTimer.Change(_pruningSamplingInterval, Timeout.InfiniteTimeSpan);
            }
        }

        void DisablePruning()
        {
            lock (_pruningTimer)
            {
                _pruningTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _pruningSampleIndex = 0;
                _pruningTimerEnabled = false;
            }
        }

        static void PruneIdleConnectors(object? state)
        {
            var pool = (ConnectorPool)state!;
            var samples = pool._pruningSamples;
            int toPrune;
            lock (pool._pruningTimer)
            {
                // Check if we might have been contending with DisablePruning.
                if (!pool._pruningTimerEnabled)
                    return;

                var sampleIndex = pool._pruningSampleIndex;
                if (sampleIndex < pool._pruningSampleSize)
                {
                    samples[sampleIndex] = pool._idle;
                    pool._pruningSampleIndex = sampleIndex + 1;
                    pool._pruningTimer.Change(pool._pruningSamplingInterval, Timeout.InfiniteTimeSpan);
                    return;
                }

                // Calculate median value for pruning, reset index and timer, and release the lock.
                Array.Sort(samples);
                toPrune = samples[pool._pruningMedianIndex];
                pool._pruningSampleIndex = 0;
                pool._pruningTimer.Change(pool._pruningSamplingInterval, Timeout.InfiniteTimeSpan);
            }

            for (var i = 0; i < toPrune && pool._numConnectors > pool._min; i++)
            {
                if (!pool.TryGetIdleConnector(out var connector))
                    return;

                pool.CloseConnector(connector);
            }
        }

        static int DivideRoundingUp(int value, int divisor) => 1 + (value - 1) / divisor;

        #endregion
    }
}
