#if !NET6_0
using System.Collections.Generic;
using Npgsql;

#pragma warning disable 1591,RS0016

// ReSharper disable once CheckNamespace
namespace System.Data.Common
{
    // public class DbBatchCommandCollection : Collection<DbBatchCommand>
    public class DbBatchCommandCollection : List<NpgsqlBatchCommand>
    {
    }
}
#endif