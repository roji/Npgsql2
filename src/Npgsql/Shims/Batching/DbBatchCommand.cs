#if !NET6_0

#pragma warning disable 1591,RS0016

// ReSharper disable once CheckNamespace
namespace System.Data.Common
{
    public abstract class DbBatchCommand
    {
        public abstract string? CommandText { get; set; }
        public abstract CommandType CommandType { get; set; }
        public abstract CommandBehavior CommandBehavior { get; set; }
        public abstract int RecordsAffected { get; set; }

        public DbParameterCollection Parameters => DbParameterCollection;

        protected abstract DbParameterCollection DbParameterCollection { get; }
    }
}
#endif