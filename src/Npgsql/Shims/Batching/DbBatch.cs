#if !NET6_0
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable 1591,RS0016

// ReSharper disable once CheckNamespace
namespace System.Data.Common
{
    public abstract class DbBatch : IDisposable, IAsyncDisposable
    {
        public DbBatchCommandCollection BatchCommands => DbBatchCommands;
        protected abstract DbBatchCommandCollection DbBatchCommands { get; }

        public DbDataReader ExecuteReader()
            => ExecuteDbDataReader();

        protected abstract DbDataReader ExecuteDbDataReader();

        public Task<DbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
            => ExecuteDbDataReaderAsync(cancellationToken);

        protected abstract Task<DbDataReader> ExecuteDbDataReaderAsync(CancellationToken cancellationToken);

        public abstract int ExecuteNonQuery();

        public abstract Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default);

        public abstract object? ExecuteScalar();

        public abstract Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken = default);

        public abstract int Timeout { get; set; }

        public DbConnection? Connection { get; set; }

        protected abstract DbConnection? DbConnection { get; set; }

        public DbTransaction? Transaction { get; set; }
        protected abstract DbTransaction? DbTransaction { get; set; }

        public abstract void Prepare();
        public abstract Task PrepareAsync(CancellationToken cancellationToken = default);
        public abstract void Cancel();

        public void Dispose() { }
        protected virtual void Dispose(bool disposing) {}
        public ValueTask DisposeAsync() => default;
    }
}
#endif