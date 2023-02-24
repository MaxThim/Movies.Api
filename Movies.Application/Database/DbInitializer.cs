using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Movies.Application.Database
{
    public class DbInitializer
    {
        private readonly IDbConnectionFactory _dbconnectionFactory;

        public DbInitializer(IDbConnectionFactory dbconnectionFactory)
        {
            _dbconnectionFactory = dbconnectionFactory;
        }

        public async Task InitializeAsync()
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();

            await connection.ExecuteAsync("""
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[movies]') AND type in (N'U'))
            CREATE TABLE [dbo].[movies] (
                [id] [uniqueidentifier] PRIMARY KEY,
                [slug] [nvarchar](450) NOT NULL,
                [title] [nvarchar](max) NOT NULL,
                [yearofrelease] [int] NOT NULL
            );
            """);

            await connection.ExecuteAsync("""
                IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'movies_slug_idx' AND object_id = OBJECT_ID('movies'))
            CREATE UNIQUE INDEX movies_slug_idx ON movies (slug);
            """);

            await connection.ExecuteAsync("""
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[genres]') AND type in (N'U'))
            CREATE TABLE [dbo].[genres](
                movieId UNIQUEIDENTIFIER REFERENCES movies (Id),
                name NVARCHAR(MAX) NOT NULL
            );
            """);
        }
    }
}
