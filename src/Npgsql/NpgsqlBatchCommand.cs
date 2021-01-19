using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Npgsql.BackendMessages;
using Npgsql.TypeMapping;
using Npgsql.Util;

#pragma warning disable 1591,RS0016

namespace Npgsql
{
    public class NpgsqlBatchCommand : DbBatchCommand
    {
        RowDescriptionMessage? _description;

        public override string? CommandText { get; set; }

        internal string? RewrittenCommandText;

        public override CommandType CommandType { get; set; } = CommandType.Text;

        public override CommandBehavior CommandBehavior { get; set; }

        public override int RecordsAffected { get; set; }

        protected override DbParameterCollection DbParameterCollection => Parameters;

        public new NpgsqlParameterCollection Parameters { get; }

        /// <summary>
        /// The input parameters sent with this statement.
        /// </summary>
        internal List<NpgsqlParameter> InputParameters { get; } = new();

        /// <summary>
        /// Specifies the type of query, e.g. SELECT.
        /// </summary>
        public StatementType StatementType { get; internal set; }

        /// <summary>
        /// For an INSERT, the object ID of the inserted row if <see cref="RecordsAffected"/> is 1 and
        /// the target table has OIDs; otherwise 0.
        /// </summary>
        public uint OID { get; internal set; }

        public NpgsqlBatchCommand()
            => Parameters = new NpgsqlParameterCollection();

        public NpgsqlBatchCommand(string? commandText)
            : this()
            => CommandText = commandText;

        internal NpgsqlBatchCommand(string? rewrittenCommandText, CommandType commandType, List<NpgsqlParameter> inputParameters)
            : this()
        {
            RewrittenCommandText = rewrittenCommandText;
            CommandType = commandType;
            InputParameters = inputParameters;
        }

        #region Known/unknown Result Types Management

        /// <summary>
        /// Fixes up the text/binary flag on result columns.
        /// Since <see cref="NpgsqlBatch.Prepare()"/> describes a statement rather than a portal, the resulting <c>RowDescription</c>
        /// will have text format on all result columns. Fix that up.
        /// </summary>
        internal void FixupRowDescription(RowDescriptionMessage rowDescription, bool isFirst)
        {
            for (var i = 0; i < rowDescription.NumFields; i++)
            {
                var field = rowDescription[i];
                field.FormatCode = AllResultTypesAreUnknown || UnknownResultTypeList?[i] == true
                    ? FormatCode.Text
                    : FormatCode.Binary;
                field.ResolveHandler();
            }
        }

        /// <summary>
        /// Marks all of the query's result columns as either known or unknown.
        /// Unknown results column are requested them from PostgreSQL in text format, and Npgsql makes no
        /// attempt to parse them. They will be accessible as strings only.
        /// </summary>
        public bool AllResultTypesAreUnknown
        {
            get => _allResultTypesAreUnknown;
            set
            {
                // TODO: Check that this isn't modified after calling prepare
                _unknownResultTypeList = null;
                _allResultTypesAreUnknown = value;
            }
        }

        bool _allResultTypesAreUnknown;

        /// <summary>
        /// Marks the query's result columns as known or unknown, on a column-by-column basis.
        /// Unknown results column are requested them from PostgreSQL in text format, and Npgsql makes no
        /// attempt to parse them. They will be accessible as strings only.
        /// </summary>
        /// <remarks>
        /// If the query includes several queries (e.g. SELECT 1; SELECT 2), this will only apply to the first
        /// one. The rest of the queries will be fetched and parsed as usual.
        ///
        /// The array size must correspond exactly to the number of result columns the query returns, or an
        /// error will be raised.
        /// </remarks>
        public bool[]? UnknownResultTypeList
        {
            get => _unknownResultTypeList;
            set
            {
                // TODO: Check that this isn't modified after calling prepare
                _allResultTypesAreUnknown = false;
                _unknownResultTypeList = value;
            }
        }

        bool[]? _unknownResultTypeList;

        #endregion

        /// <summary>
        /// The RowDescription message for this query. If null, the query does not return rows (e.g. INSERT)
        /// </summary>
        internal RowDescriptionMessage? Description
        {
            get => PreparedStatement == null ? _description : PreparedStatement.Description;
            set
            {
                if (PreparedStatement == null)
                    _description = value;
                else
                    PreparedStatement.Description = value;
            }
        }

        internal void ValidateParameters(ConnectorTypeMapper typeMapper)
        {
            for (var i = 0; i < Parameters.Count; i++)
            {
                var p = Parameters[i];
                if (!p.IsInputDirection)
                    continue;
                p.Bind(typeMapper);
                p.LengthCache?.Clear();
                p.ValidateAndGetLength();
            }
        }

        /// <summary>
        /// If this command has been prepared, references the <see cref="PreparedStatement"/>, otherwise <see langword="null" />.
        /// </summary>
        internal PreparedStatement? PreparedStatement
        {
            get => _preparedStatement != null && _preparedStatement.State == PreparedState.Unprepared
                ? _preparedStatement = null
                : _preparedStatement;
            set => _preparedStatement = value;
        }

        PreparedStatement? _preparedStatement;

        // TODO: See about killing this
        internal bool IsPreparing;

        /// <summary>
        /// Holds the server-side (prepared) statement name. Empty string for non-prepared statements.
        /// </summary>
        internal string StatementName => PreparedStatement?.Name ?? "";

        /// <summary>
        /// Whether this statement has already been prepared (including automatic preparation).
        /// </summary>
        internal bool IsPrepared => PreparedStatement?.IsPrepared == true;

        internal void Reset()
        {
            CommandText = string.Empty;
            InputParameters.Clear();
            StatementType = StatementType.Select;
            _description = null;
            RecordsAffected = 0;
            OID = 0;
            Parameters.Clear();
            PreparedStatement = null;
        }

        internal void ApplyCommandComplete(CommandCompleteMessage msg)
        {
            StatementType = msg.StatementType;
            RecordsAffected = (int)msg.Rows; // TODO: LongRecordsAffected?
            OID = msg.OID;
        }

        internal void RewriteCommand(bool standardConformingStrings)
        {
            NpgsqlBatchCommandCollection? innerBatchCommands = null;

            var commandRewriter = new CommandRewriter();
            commandRewriter.RewriteCommand(
                CommandText,
                CommandType,
                Parameters,
                out RewrittenCommandText,
                InputParameters,
                ref innerBatchCommands,
                standardConformingStrings);

            if (innerBatchCommands is not null)
            {
                throw new NotSupportedException(
                    $"Specifying multiple SQL statements in a single {nameof(NpgsqlBatchCommand)} isn't supported, " +
                    "please remove all semicolons.");
            }
        }

        /// <summary>
        /// Returns the SQL text of the statement.
        /// </summary>
        public override string ToString() => CommandText ?? "<none>";
    }
}
