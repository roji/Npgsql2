using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests
{
    [TestFixture(MultiplexingMode.NonMultiplexing, BatchOrCommand.Batch)]
    [TestFixture(MultiplexingMode.NonMultiplexing, BatchOrCommand.Command)]
    [TestFixture(MultiplexingMode.Multiplexing, BatchOrCommand.Batch)]
    [TestFixture(MultiplexingMode.Multiplexing, BatchOrCommand.Command)]
    public class BatchingTests : MultiplexingTestBase
    {
        public BatchingTests(MultiplexingMode multiplexingMode, BatchOrCommand batchOrCommand)
            : base(multiplexingMode)
            => _batchOrCommand = batchOrCommand;

        [Test]
        public async Task Combinations(
            [Values(
                new[] { true }, new[] { false },
                new[] { true, true }, new[] { false, false },
                new[] { true, false}, new[] { false, true })] bool[] queries,
            [Values] PrepareOrNot prepare)
        {
            if (IsMultiplexing && _batchOrCommand == BatchOrCommand.Command && queries.Length > 1)
                return;

            using var conn = await OpenConnectionAsync();

            var batch = new NpgsqlBatch(conn);
            batch.BatchCommands.AddRange(queries.Select(q => new NpgsqlBatchCommand(q
                ? "SELECT 1"
                : "SET lock_timeout = 1000")));
            if (prepare == PrepareOrNot.Prepared)
            {
                if (IsMultiplexing)
                    return;
                await batch.PrepareAsync();
            }

            using var reader = await ExecuteReaderAs(batch);
            var numResultSets = queries.Count(q => q);
            for (var i = 0; i < numResultSets; i++)
            {
                Assert.That(reader.Read(), Is.True);
                Assert.That(reader[0], Is.EqualTo(1));
                Assert.That(reader.NextResult(), Is.EqualTo(i != numResultSets - 1));
            }
        }

        [Test]
        public async Task With_parameters(
            [Values] BatchOrCommand batchOrCommand,
            [Values] PrepareOrNot prepare)
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = {
                    new("SELECT @p1") { Parameters = { new("p1", 8) } },
                    new("SELECT @p2") { Parameters = { new("p2", 9) } }
                }
            };

            using var reader = await ExecuteReaderAs(batch);

            Assert.That(await reader.ReadAsync(), Is.True);
            Assert.That(reader.GetInt32(0), Is.EqualTo(8));
            Assert.That(await reader.ReadAsync(), Is.False);
            Assert.That(await reader.NextResultAsync(), Is.True);

            Assert.That(await reader.ReadAsync(), Is.True);
            Assert.That(reader.GetInt32(0), Is.EqualTo(9));
            Assert.That(await reader.ReadAsync(), Is.False);
            Assert.That(await reader.NextResultAsync(), Is.False);
        }

        Task<NpgsqlDataReader> ExecuteReaderAs(
            NpgsqlBatch batch, CancellationToken cancellationToken = default)
        {
            if (_batchOrCommand == BatchOrCommand.Batch)
                return batch.ExecuteReaderAsync(cancellationToken);

            var command = new NpgsqlCommand(string.Join("; ", batch.BatchCommands.Select(b => b.CommandText)), batch.Connection);
            command.Parameters.AddRange(batch.BatchCommands.SelectMany(c => c.Parameters).ToArray());
            return command.ExecuteReaderAsync(cancellationToken);
        }

        readonly BatchOrCommand _batchOrCommand;
    }
}
