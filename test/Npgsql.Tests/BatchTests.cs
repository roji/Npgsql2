using System;
using System.Data;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Npgsql.Tests
{
    public class BatchTests : MultiplexingTestBase
    {
        public BatchTests(MultiplexingMode multiplexingMode) : base(multiplexingMode) {}

        [Test]
        public async Task Empty()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn);
            using var reader = await batch.ExecuteReaderAsync();

            Assert.False(reader.Read());
            Assert.False(reader.NextResult());
        }

        [Test]
        public async Task ExecuteScalar_single_command()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn) { BatchCommands = { new("SELECT 1") } };
            Assert.That(await batch.ExecuteScalarAsync(), Is.EqualTo(1));
        }

        [Test]
        public async Task ExecuteScalar_multiple_commands()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn) { BatchCommands =
            {
                new("SELECT 1"),
                new("SELECT 2")
            } };
            Assert.That(await batch.ExecuteScalarAsync(), Is.EqualTo(1));
        }

        [Test]
        public async Task ExecuteNonQuery_single_command()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn) { BatchCommands = { new("SELECT 1") } };
            Assert.That(await batch.ExecuteNonQueryAsync(), Is.EqualTo(-1));
        }

        [Test]
        public async Task ExecuteNonQuery_multiple_commands()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn) { BatchCommands =
            {
                new("SELECT 1"),
                new("SELECT 2")
            } };
            Assert.That(await batch.ExecuteNonQueryAsync(), Is.EqualTo(-1));
        }

        [Test]
        public async Task Single_command()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = { new("SELECT @p") { Parameters = { new("p", 8) }} }
            };

            using var reader = await batch.ExecuteReaderAsync();

            Assert.That(await reader.ReadAsync(), Is.True);
            Assert.That(reader.GetInt32(0), Is.EqualTo(8));
            Assert.That(await reader.NextResultAsync(), Is.False);
        }

        [Test]
        public async Task Nested_batches_are_not_supported()
        {
            using var conn = await OpenConnectionAsync();
            using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = { new("SELECT 1; SELECT 2") }
            };

            Assert.That(() => batch.ExecuteReader(), Throws.Exception.TypeOf<NotSupportedException>());
        }

        [Test]
        public void Default_BatchCommand_CommandType_is_Text()
            => Assert.That(new NpgsqlBatchCommand().CommandType, Is.EqualTo(CommandType.Text));
    }
}
