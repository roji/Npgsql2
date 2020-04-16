#if !NET461

using System.Text.Utf8;
using System.Threading.Tasks;
using Npgsql.Tests;
using NUnit.Framework;

namespace Npgsql.PluginTests
{
    public class Utf8StringTests : TestBase
    {
        [Test]
        public async Task Utf8String()
        {
            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT 'hello world'::text", conn);
            using var reader = await cmd.ExecuteReaderAsync();

            await reader.ReadAsync();
            Assert.That(reader.GetFieldValue<Utf8String>(0), Is.EqualTo(new Utf8String("hello world")));
        }

        protected override NpgsqlConnection OpenConnection(string? connectionString = null)
        {
            var conn = new NpgsqlConnection(connectionString ?? ConnectionString);
            conn.Open();
            conn.TypeMapper.UseUtf8String();
            return conn;
        }
    }
}

#endif
