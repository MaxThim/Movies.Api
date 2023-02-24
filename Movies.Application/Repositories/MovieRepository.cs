using Dapper;
using Movies.Application.Database;
using Movies.Application.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Movies.Application.Repositories
{
    public class MovieRepository : IMovieRepository
    {
        private readonly IDbConnectionFactory _dbconnectionFactory;

        public MovieRepository(IDbConnectionFactory dbconnectionFactory)
        {
            _dbconnectionFactory = dbconnectionFactory;
        }

        public async Task<bool> CreateAsync(Movie movie)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();

            using var transaction = connection.BeginTransaction();

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                insert into movies (id, slug, title, yearofrelease)
                values (@Id, @Slug, @Title, @YearOfRelease)
                """, movie, transaction));

            if (result > 0)
            {
                foreach (var genre in movie.Genres)
                {
                    await connection.ExecuteAsync(new CommandDefinition("""
                        insert into genres (movieId, name)
                        values (@MovieId, @Name)
                        """, new {MovieId = movie.Id, Name = genre}, transaction));
                }
            }
            transaction.Commit();

            return result > 0;
        }

        public async Task<bool> DeleteByIdAsync(Guid id)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            using var transaction = connection.BeginTransaction();

            await connection.ExecuteAsync(new CommandDefinition("""
                delete from genres where movieid = @id
                """, new { id }, transaction));

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                delete from movies where id = @id
                """, new { id }, transaction));

            transaction.Commit();

            return result > 0;
        }

        public async Task<bool> ExistsByIdAsync(Guid id)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            return await connection.ExecuteScalarAsync<bool>(new CommandDefinition("""
                select count(1) from movies where id = @id
                """, new { id }));

        }

        public async Task<IEnumerable<Movie>> GetAllAsync()
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();

            var result = await connection.QueryAsync(
                @"SELECT m.*, STRING_AGG(g.name, ',') as genres
                FROM movies m
                LEFT JOIN genres g ON m.id = g.movieId
                GROUP BY m.id, m.title, m.slug, m.yearofrelease
                ORDER BY m.id");

            return result.Select(x => new Movie
            {
                Id = x.id,
                Title = x.title,
                YearOfRelease = x.yearofrelease,
                Genres = Enumerable.ToList(x.genres.Split(','))
            });
        }

        public async Task<Movie?> GetByIdAsync(Guid id)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            var movie = await connection.QuerySingleOrDefaultAsync<Movie>(
                new CommandDefinition("""
                    Select * FROM movies WHERE id = @Id
                    """, new { id }
                ));

            if (movie == null)
            {
                return null;
            }

            var genres = await connection.QueryAsync<string>(
                new CommandDefinition("""
                    select name from genres where movieid = @id
                    """, new { id }));

            foreach (var genre in genres)
            {
                movie.Genres.Add(genre);
            }

            return movie;
        }

        public async Task<Movie?> GetBySlugAsync(string slug)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            var movie = await connection.QuerySingleOrDefaultAsync<Movie>(
                new CommandDefinition("""
                    Select * FROM movies WHERE slug = @Slug
                    """, new { slug }
                ));

            if (movie == null)
            {
                return null;
            }

            var genres = await connection.QueryAsync<string>(
                new CommandDefinition("""
                    select name from genres where movieid = @id
                    """, new { id = movie.Id }));

            foreach (var genre in genres)
            {
                movie.Genres.Add(genre);
            }

            return movie;
        }

        public async Task<bool> UpdateAsync(Movie movie)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            using var transaction = connection.BeginTransaction();

            await connection.ExecuteAsync(new CommandDefinition("""
                delete from genres where movieid = @id
                """, new { id = movie.Id }, transaction));

            foreach (var genre in movie.Genres)
            {
                await connection.ExecuteAsync(new CommandDefinition("""
                    insert into genres (movieId, name)
                    values (@MovieId, @Name)
                    """, new { MovieId = movie.Id, Name = genre }, transaction));
            }

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                    update movies
                    set slug = @Slug, title = @Title, yearofrelease = @YearOfRelease
                    where id = @Id
                    """, movie, transaction));

            transaction.Commit();
            return result > 0;
        }
    }
}
