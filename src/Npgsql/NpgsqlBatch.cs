using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Npgsql
{
    public class NpgsqlBatch : DbBatch
    {
        readonly NpgsqlCommand _command;

        protected override DbBatchCommandCollection DbBatchCommands => BatchCommands;

        public new NpgsqlBatchCommandCollection BatchCommands { get; }

        public NpgsqlBatch(NpgsqlConnection? connection = null, NpgsqlTransaction? transaction = null)
        {
            Connection = connection;
            Transaction = transaction;

            // TODO: Construct underlying command with InternalBatchCommands of length 5
            var batchCommands = new List<NpgsqlBatchCommand>(5);
            _command = new(batchCommands);
            BatchCommands = new NpgsqlBatchCommandCollection(batchCommands);
        }

        protected override DbBatchCommand CreateDbBatchCommand()
            => new NpgsqlBatchCommand();

        #region ExecuteReader

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            => ExecuteReader(behavior);

        public new NpgsqlDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
            => _command.ExecuteReader();

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken)
            => await ExecuteReaderAsync(cancellationToken);

        public new Task<NpgsqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
            => _command.ExecuteReaderAsync(cancellationToken);

        public new Task<NpgsqlDataReader> ExecuteReaderAsync(
            CommandBehavior behavior,
            CancellationToken cancellationToken = default)
            => _command.ExecuteReaderAsync(behavior, cancellationToken);

        #endregion ExecuteReader

        public override int ExecuteNonQuery()
            => _command.ExecuteNonQuery();

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
            => _command.ExecuteNonQueryAsync(cancellationToken);

        public override object? ExecuteScalar()
            => _command.ExecuteScalar();

        public override Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default)
            => _command.ExecuteScalarAsync(cancellationToken);

        public override void Prepare()
            => _command.Prepare();

        public override Task PrepareAsync(CancellationToken cancellationToken = default)
            => _command.PrepareAsync(cancellationToken);

        #region Passthrough to command

        public override int Timeout
        {
            get => _command.CommandTimeout;
            set => _command.CommandTimeout = value;
        }

        [DefaultValue(null)]
        [Category("Behavior")]
        public new NpgsqlConnection? Connection
        {
            get => _command.Connection;
            set => _command.Connection = value;
        }

        // TODO: Probably incorrect - set _connection directly on the command?
        // Why do we even have two covariant properties here? Check and compare to NpgsqlCommand.
        protected override DbConnection? DbConnection
        {
            get => _command.Connection;
            set => _command.Connection = (NpgsqlConnection?)value;
        }

        // TODO: Same as DbConnection
        protected override DbTransaction? DbTransaction
        {
            get => _command.Transaction;
            set => _command.Transaction = (NpgsqlTransaction?)value;
        }

        public override void Cancel() => _command.Cancel();

        #endregion Passthrough to command
    }
}
