using System;
using Npgsql;

namespace ConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            using var conn = new NpgsqlConnection("Host=localhost;Username=test;Password=test");
            conn.Open();

            using var cmd = new NpgsqlCommand("SELECT 1", conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
            }
        }
    }
}
