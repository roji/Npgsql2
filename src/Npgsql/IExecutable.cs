using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Util;

namespace Npgsql
{
    /// <summary>
    /// A common interface over <see cref="NpgsqlCommand"/> and <see cref="NpgsqlBatch"/>.
    /// </summary>
    interface IExecutable
    {
        NpgsqlConnection? Connection { get; }
        CommandState State { set; }
        ManualResetValueTaskSource<NpgsqlConnector> ExecutionCompletion { get; }
        void AutoPrepare(NpgsqlConnector connector);
        Task SendExecute(NpgsqlConnector connector, bool async, CancellationToken cancellationToken = default);
        Type[]? ObjectResultTypes { get; }
    }
}
