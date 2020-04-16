using System;
using System.Data;
using Npgsql.TypeHandlers.DateTimeHandlers;
using Npgsql.TypeMapping;
using NpgsqlTypes;

// ReSharper disable once CheckNamespace
namespace Npgsql
{
    /// <summary>
    /// Extension adding the Utf8String plugin to an Npgsql type mapper.
    /// </summary>
    public static class NpgsqlUtf8StringExtensions
    {
        /// <summary>
        /// Sets up Utf8String mappings.
        /// </summary>
        public static INpgsqlTypeMapper UseUtf8String(this INpgsqlTypeMapper mapper)
            => mapper
                .AddMapping(new NpgsqlTypeMappingBuilder
                {
                    PgTypeName = "text",
                    NpgsqlDbType = NpgsqlDbType.Text,
                    DbTypes = new[] { DbType.String, DbType.StringFixedLength, DbType.AnsiString, DbType.AnsiStringFixedLength },
                    ClrTypes = new[] { typeof(string) },
                    InferredDbType = DbType.String,
                    TypeHandlerFactory = new Utf8TextHandlerFactory()
                }.Build());
    }
}
