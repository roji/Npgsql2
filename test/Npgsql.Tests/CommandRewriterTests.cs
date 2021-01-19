using System.Collections.Generic;
using System.Data;
using System.Linq;
using NUnit.Framework;

namespace Npgsql.Tests
{
    class CommandRewriterTests
    {
        [Test]
        public void Param()
        {
            var parameters = new NpgsqlParameterCollection
            {
                new("p1", "foo"),
                new("p2", "bar")
            };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite("SELECT @p1, :p2", parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands);

            Assert.That(rewrittenSql, Is.EqualTo("SELECT $1, $2"));
            Assert.That(reorderedParameters, Has.Count.EqualTo(2));
            Assert.That(reorderedParameters[0], Is.SameAs(parameters[0]));
            Assert.That(reorderedParameters[1], Is.SameAs(parameters[1]));
        }

        [Test]
        public void Params_in_reversed_order()
        {
            var parameters = new NpgsqlParameterCollection
            {
                new("p2", "bar"),
                new("p1", "foo")
            };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite("SELECT @p1, :p2", parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands);

            Assert.That(rewrittenSql, Is.EqualTo("SELECT $1, $2"));
            Assert.That(reorderedParameters, Has.Count.EqualTo(2));
            Assert.That(reorderedParameters[0], Is.SameAs(parameters[1]));
            Assert.That(reorderedParameters[1], Is.SameAs(parameters[0]));
        }

        [Test, Description("Checks several scenarios in which the SQL is supposed to pass untouched")]
        [TestCase(@"SELECT 1", TestName = "NothingSpecial")]
        [TestCase(@"SELECT * FROM (SELECT 1; SELECT 1)", TestName = "SemicolonInParentheses")]
        [TestCase(@"SELECT to_tsvector('fat cats ate rats') @@ to_tsquery('cat & rat')", TestName = "AtAt")]
        [TestCase(@"SELECT 'cat'::tsquery @> 'cat & rat'::tsquery", TestName = "AtGt")]
        [TestCase(@"SELECT 'cat'::tsquery <@ 'cat & rat'::tsquery", TestName = "AtLt")]
        [TestCase(@"SELECT 'b''la'", TestName = "DoubleTicks")]
        [TestCase(@"SELECT 'type(''m.response'')#''O''%'", TestName = "DoubleTicks2")]
        [TestCase(@"SELECT 'abc'':str''a:str'", TestName = "DoubleTicks3")]
        [TestCase(@"SELECT 1 FROM "":str""", TestName = "DoubleQuoted")]
        [TestCase(@"SELECT 1 FROM 'yo'::str", TestName = "DoubleColons")]
        [TestCase("SELECT $\u00ffabc0$literal string :str :int$\u00ffabc0 $\u00ffabc0$", TestName = "DollarQuotes")]
        [TestCase("SELECT $$:str$$", TestName = "DollarQuotesNoTag")]
        public void Sql_should_not_be_rewritten(string sql)
        {
            var parameters = new NpgsqlParameterCollection { new("p1", "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite(sql, parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands);

            Assert.That(rewrittenSql, Is.EqualTo(sql));
            Assert.That(reorderedParameters, Is.Empty);
        }

        [Test]
        [TestCase(@"SELECT 1<:param", "param", TestName = "LessThan")]
        [TestCase(@"SELECT 1>:param", "param", TestName = "GreaterThan")]
        [TestCase(@"SELECT 1<>:param", "param", TestName = "NotEqual")]
        [TestCase("SELECT--comment\r:param", "param", TestName = "LineComment")]
        [TestCase(@"SELECT :a.parameter", "a.parameter", TestName = "NameWithDot")]
        [TestCase(@"SELECT :param", ":param", TestName = "PlaceholderPrefix")]
        [TestCase(@"SELECT :漢字", "漢字", TestName = "PlaceholderPrefix")] // #1177
        public void Param_gets_rewritten(string sql, string paramName)
        {
            var parameters = new NpgsqlParameterCollection { new(paramName, "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite(sql, parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands);

            Assert.That(rewrittenSql, Does.Contain("$1"));
            Assert.That(reorderedParameters.Single(), Is.SameAs(parameters.Single()));
        }

        [Test]
        [TestCase(@"SELECT '@param'", TestName = "InsideString")]
        [TestCase(@"SELECT @unknown", TestName = "UnmatchedPlaceholder")]
        [TestCase(@"SELECT e'ab\'c:param'", TestName = "Estring")]
        [TestCase(@"SELECT/*/* -- nested comment :int /*/* *//*/ **/*/*/*/1")]
        [TestCase(@"SELECT 1,
-- Comment, @param and also :param
2", TestName = "LineComment")]
        public void Param_doesnt_get_rewritten(string sql)
        {
            var parameters = new NpgsqlParameterCollection { new("param", "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite(sql, parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands);

            Assert.That(rewrittenSql, Does.Not.Contain("$1"));
            Assert.That(reorderedParameters, Is.Empty);
        }

        [Test]
        public void Standard_conforming_strings()
        {
            var parameters = new NpgsqlParameterCollection { new("param", "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite(@"SELECT 'abc\@param'", parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands);

            Assert.That(rewrittenSql, Is.EqualTo(@"SELECT 'abc$1'"));
            // Assert.That(reorderedParameters, Is.Empty);
        }

        [Test]
        public void Standard_conforming_strings_off()
        {
            var parameters = new NpgsqlParameterCollection { new("param", "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();

            Rewrite(@"SELECT 'abc\@param'", parameters, out var rewrittenSql, reorderedParameters, ref _dummyBatchCommands,
                standardConformingStrings: false);

            Assert.That(rewrittenSql, Is.EqualTo(@"SELECT 'abc\@param'"));
            Assert.That(reorderedParameters, Is.Empty);
        }

        [Test]
        public void Output_parameter_throws()
        {
            var parameters = new NpgsqlParameterCollection { new("param", "foo") { Direction = ParameterDirection.Output } };
            var reorderedParameters = new List<NpgsqlParameter>();

            Assert.That(
                () => Rewrite(@"SELECT @param", parameters, out _, reorderedParameters, ref _dummyBatchCommands),
                Throws.Exception);
        }

        [Test]
        public void Parse_twice()
        {
            var rewriter = new CommandRewriter();

            var parameters = new NpgsqlParameterCollection { new("p", "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            rewriter.RewriteCommand(
                "SELECT @p", CommandType.Text, parameters, out var rewrittenSql, reorderedParameters, ref batchCommands, true);
            Assert.That(rewrittenSql, Is.EqualTo("SELECT $1"));
            Assert.That(reorderedParameters.Single(), Is.SameAs(parameters.Single()));

            parameters[0].ParameterName = "q";
            rewriter.RewriteCommand(
                "SELECT @q -- foo", CommandType.Text, parameters, out rewrittenSql, reorderedParameters, ref batchCommands, true);
            Assert.That(rewrittenSql, Is.EqualTo("SELECT $1 -- foo"));
            Assert.That(reorderedParameters.Single(), Is.SameAs(parameters.Single()));
        }

        [Test]
        public void Single_command()
        {
            var parameters = new NpgsqlParameterCollection { new("p", "foo") };
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite("SELECT @p", parameters, out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.EqualTo("SELECT $1"));
            Assert.That(reorderedParameters.Single(), Is.SameAs(parameters.Single()));
            Assert.That(batchCommands, Is.Null);
        }

        [Test]
        public void Single_command_with_trailing_semicolon_and_whitespace()
        {
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite("SELECT 1;  ", new NpgsqlParameterCollection(), out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.EqualTo("SELECT 1"));
            Assert.That(batchCommands, Is.Null);
        }

        [Test]
        public void Multiple_commands()
        {
            var parameters = new NpgsqlParameterCollection
            {
                new("p1", "foo"),
                new("p2", "bar")
            };
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite("SELECT @p1; SELECT @p2", parameters, out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.Null);
            // reorderParameters shouldn't be consulted

            Assert.That(batchCommands, Has.Count.EqualTo(2));
            Assert.That(batchCommands![0].CommandText, Is.Null);
            Assert.That(batchCommands[0].RewrittenCommandText, Is.EqualTo("SELECT $1"));
            Assert.That(batchCommands[0].InputParameters.Single(), Is.SameAs(parameters[0]));
            Assert.That(batchCommands[1].CommandText, Is.Null);
            Assert.That(batchCommands[1].RewrittenCommandText, Is.EqualTo("SELECT $1"));
            Assert.That(batchCommands[1].InputParameters.Single(), Is.SameAs(parameters[1]));
        }

        [Test]
        public void Multiple_commands_with_recycled_NpgsqlBatchCommandCollection()
        {
            var parameters = new NpgsqlParameterCollection
            {
                new("p1", "foo"),
                new("p2", "bar")
            };
            var reorderedParameters = new List<NpgsqlParameter>();
            var batchCommands = new NpgsqlBatchCommandCollection
            {
                new(), new(), new()
            };

            Rewrite("SELECT @p1; SELECT @p2", parameters, out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.Null);

            Assert.That(batchCommands, Has.Count.EqualTo(2));
            Assert.That(batchCommands![0].RewrittenCommandText, Is.EqualTo("SELECT $1"));
            Assert.That(batchCommands[0].InputParameters.Single(), Is.SameAs(parameters[0]));
            Assert.That(batchCommands[1].RewrittenCommandText, Is.EqualTo("SELECT $1"));
            Assert.That(batchCommands[1].InputParameters.Single(), Is.SameAs(parameters[1]));
        }

        [Test]
        public void Multiple_commands_with_mixed_up_parameters()
        {
            var parameters = new NpgsqlParameterCollection
            {
                new("p1", "foo"),
                new("p2", "bar"),
                new("p3", "baz")
            };
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite("SELECT @p3, @p1; SELECT @p2, @p3", parameters, out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.Null);

            Assert.That(batchCommands, Has.Count.EqualTo(2));

            Assert.That(batchCommands![0].RewrittenCommandText, Is.EqualTo("SELECT $1, $2"));
            Assert.That(batchCommands[0].InputParameters, Has.Count.EqualTo(2));
            Assert.That(batchCommands[0].InputParameters[0], Is.SameAs(parameters[2]));
            Assert.That(batchCommands[0].InputParameters[1], Is.SameAs(parameters[0]));

            Assert.That(batchCommands[1].RewrittenCommandText, Is.EqualTo("SELECT $1, $2"));
            Assert.That(batchCommands[1].InputParameters, Has.Count.EqualTo(2));
            Assert.That(batchCommands[1].InputParameters[0], Is.SameAs(parameters[1]));
            Assert.That(batchCommands[1].InputParameters[1], Is.SameAs(parameters[2]));
        }

        [Test]
        public void Multiple_commands_ending_with_semicolon()
        {
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite("SELECT 1; SELECT 2;   ", new NpgsqlParameterCollection(), out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.Null);

            Assert.That(batchCommands, Has.Count.EqualTo(2));
            Assert.That(batchCommands![0].RewrittenCommandText, Is.EqualTo("SELECT 1"));
            Assert.That(batchCommands[1].RewrittenCommandText, Is.EqualTo("SELECT 2"));
        }

        [Test]
        public void SemicolonAfterParentheses()
        {
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite(
                "CREATE OR REPLACE RULE test AS ON UPDATE TO test DO (SELECT 1); SELECT 1",
                new NpgsqlParameterCollection(), out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.Null);
            Assert.That(batchCommands, Has.Count.EqualTo(2));
            Assert.That(batchCommands![0].RewrittenCommandText, Is.EqualTo("CREATE OR REPLACE RULE test AS ON UPDATE TO test DO (SELECT 1)"));
            Assert.That(batchCommands[1].RewrittenCommandText, Is.EqualTo("SELECT 1"));
        }

        [Test]
        public void ConsecutiveSemicolons()
        {
            var reorderedParameters = new List<NpgsqlParameter>();
            NpgsqlBatchCommandCollection? batchCommands = null;

            Rewrite(
                ";;SELECT 1", new NpgsqlParameterCollection(), out var rewrittenSql, reorderedParameters, ref batchCommands);

            Assert.That(rewrittenSql, Is.Null);
            Assert.That(batchCommands, Has.Count.EqualTo(3));
            Assert.That(batchCommands![0].RewrittenCommandText, Is.Empty);
            Assert.That(batchCommands[1].RewrittenCommandText, Is.Empty);
            Assert.That(batchCommands[2].RewrittenCommandText, Is.EqualTo("SELECT 1"));
        }

        static void Rewrite(
            string? commandText,
            NpgsqlParameterCollection parameters,
            out string? rewrittenCommandText,
            List<NpgsqlParameter> inputParameters,
            ref NpgsqlBatchCommandCollection? batchCommands,
            bool standardConformingStrings = true,
            bool deriveParameters = false)
            => new CommandRewriter()
                .RewriteCommand(commandText, CommandType.Text, parameters, out rewrittenCommandText, inputParameters, ref batchCommands,
                    standardConformingStrings, deriveParameters);

        NpgsqlBatchCommandCollection? _dummyBatchCommands;
    }
}
