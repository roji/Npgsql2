using System.Diagnostics;
using System.IO;

namespace Npgsql.Util
{
    static class CrappyLog
    {
#if DEBUG
        static TextWriter _writer;

        static CrappyLog() => _writer = File.CreateText("/tmp/log.txt");
#endif

        [Conditional("DEBUG")]
        public static void Write(string text)
        {
#if DEBUG
            lock (_writer)
            {
                _writer.WriteLine(text);
                _writer.Flush();
            }
#endif
        }
    }
}
