using System;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandling;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Npgsql
{
    public class Utf8TextHandlerFactory : NpgsqlTypeHandlerFactory<Utf8String>
    {
        public override NpgsqlTypeHandler<Utf8String> Create(PostgresType postgresType, NpgsqlConnection conn)
            => new Utf8TextHandler(postgresType);
    }

    public class Utf8TextHandler : NpgsqlTypeHandler<Utf8String>
    {
        /// <inheritdoc />
        protected internal Utf8TextHandler(PostgresType postgresType)
            : base(postgresType)
        {
            // TODO: Check that the connector encoding is UTF8
        }

        public override ValueTask<Utf8String> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription? fieldDescription = null)
        {
            if (len <= buf.ReadBytesLeft)
                return new ValueTask<Utf8String>(new Utf8String(buf.ReadSpan(len)));

            throw new NotImplementedException("Can't read huge UTF8 strings yet");
        }

        public override int ValidateAndGetLength(Utf8String value, ref NpgsqlLengthCache? lengthCache,  NpgsqlParameter? parameter)
        {
            // TODO: truncating via parameter.Size, but is that bytes or chars...
            return value.Length;
        }

        public override Task Write(Utf8String value, NpgsqlWriteBuffer buf, NpgsqlLengthCache? lengthCache,
            NpgsqlParameter? parameter,
            bool async)
        {
            var span = value.AsBytes();

            // The entire string fits in our buffer, copy it as usual.
            if (span.Length <= buf.WriteSpaceLeft)
            {
                buf.WriteBytes(span);
                return Task.CompletedTask;
            }

            return WriteAsync();

            async Task WriteAsync()
            {
                // The segment is larger than our buffer. Flush whatever is currently in the buffer and
                // write the array directly to the socket.
                await buf.Flush(async);
                await buf.DirectWrite(value.AsMemoryBytes(), async);
            }
        }
    }
}
