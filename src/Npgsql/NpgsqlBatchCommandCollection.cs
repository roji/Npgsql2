using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Npgsql
{
    public class NpgsqlBatchCommandCollection : DbBatchCommandCollection, IList<NpgsqlBatchCommand>
    {
        readonly List<NpgsqlBatchCommand> _list;

        internal NpgsqlBatchCommandCollection(List<NpgsqlBatchCommand> batchCommands)
            => _list = batchCommands;

        public override int Count => _list.Count;

        public override bool IsReadOnly => false;

        IEnumerator<NpgsqlBatchCommand> IEnumerable<NpgsqlBatchCommand>.GetEnumerator() => _list.GetEnumerator();

        public override IEnumerator<DbBatchCommand> GetEnumerator() => _list.GetEnumerator();

        public void Add(NpgsqlBatchCommand item) => _list.Add(item);

        public override void Add(DbBatchCommand item) => Add(Cast(item));

        public override void Clear() => _list.Clear();

        public bool Contains(NpgsqlBatchCommand item) => _list.Contains(item);

        public override bool Contains(DbBatchCommand item) => Contains(Cast(item));

        public void CopyTo(NpgsqlBatchCommand[] array, int arrayIndex) => _list.CopyTo(array, arrayIndex);

        public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        {
            if (array is NpgsqlBatchCommand[] typedArray)
            {
                CopyTo(typedArray, arrayIndex);
                return;
            }

            throw new InvalidCastException(
                $"{nameof(array)} is not of type {nameof(NpgsqlBatchCommand)} and cannot be used in this batch command collection.");
        }

        public int IndexOf(NpgsqlBatchCommand item) => _list.IndexOf(item);

        public override int IndexOf(DbBatchCommand item) => IndexOf(Cast(item));

        public void Insert(int index, NpgsqlBatchCommand item) => _list.Insert(index, item);

        public override void Insert(int index, DbBatchCommand item) => Insert(index, Cast(item));

        public bool Remove(NpgsqlBatchCommand item) => _list.Remove(item);

        public override bool Remove(DbBatchCommand item) => Remove(Cast(item));

        public override void RemoveAt(int index) => _list.RemoveAt(index);

        NpgsqlBatchCommand IList<NpgsqlBatchCommand>.this[int index]
        {
            get => _list[index];
            set => _list[index] = Cast(value);
        }

        public override DbBatchCommand this[int index]
        {
            get => _list[index];
            set => _list[index] = Cast(value);
        }

        static NpgsqlBatchCommand Cast(object? value)
            => value is NpgsqlBatchCommand c
                ? c
                : throw new InvalidCastException(
                    $"The value \"{value}\" is not of type \"{nameof(NpgsqlBatchCommand)}\" and cannot be used in this batch command collection.");
    }
}
