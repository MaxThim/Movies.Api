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

        public async Task<bool> CreateAsync(Movie movie, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();

            using var transaction = connection.BeginTransaction();

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                insert into movies (id, slug, title, yearofrelease)
                values (@Id, @Slug, @Title, @YearOfRelease)
                """, movie, transaction, cancellationToken: token));

            if (result > 0)
            {
                foreach (var genre in movie.Genres)
                {
                    await connection.ExecuteAsync(new CommandDefinition("""
                        insert into genres (movieId, name)
                        values (@MovieId, @Name)
                        """, new {MovieId = movie.Id, Name = genre}, transaction, cancellationToken: token));
                }
            }
            transaction.Commit();

            return result > 0;
        }

        public async Task<bool> DeleteByIdAsync(Guid id, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            using var transaction = connection.BeginTransaction();

            await connection.ExecuteAsync(new CommandDefinition("""
                delete from genres where movieid = @id
                """, new { id }, transaction, cancellationToken: token));

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                delete from movies where id = @id
                """, new { id }, transaction, cancellationToken: token));

            transaction.Commit();

            return result > 0;
        }

        public async Task<bool> ExistsByIdAsync(Guid id, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            return await connection.ExecuteScalarAsync<bool>(new CommandDefinition("""
                select count(1) from movies where id = @id
                """, new { id }, cancellationToken: token));

        }

        public async Task<IEnumerable<Movie>> GetAllAsync(GetAllMoviesOptions options, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();

            var orderClause = string.Empty;
            if (options.SortField != null)
            {
                orderClause = $"""
                    , m.{options.SortField} 
                    order by m.{options.SortField} {(options.SortOrder == SortOrder.Ascending ? "asc" : "desc")}
                    """;
            }

            var result = await connection.QueryAsync(new CommandDefinition(
                @$"SELECT m.*, STRING_AGG(g.name, ',') as genres,
                AVG(CAST(r.rating AS float)) as rating, myr.rating as userrating
                FROM movies m
                LEFT JOIN genres g ON m.id = g.movieId
                LEFT JOIN ratings r on m.id = r.movieid
                LEFT JOIN ratings myr on m.id = myr.movieid AND myr.userid = @userId
                WHERE (@Title is null or m.title like '%' + @Title + '%')
                AND (@YearOfRelease is null or m.yearofrelease = @YearOfRelease)
                GROUP BY m.id, m.title, m.slug, m.yearofrelease, myr.rating {orderClause} 
                OFFSET @PageOffSet ROWS 
                FETCH NEXT @Page ROWS ONLY", 
                new 
                { 
                    userId = options.UserId,
                    Title = options.Title,
                    YearOfRelease = options.YearOfRelease,
                    PageOffSet = (options.Page - 1) * options.PageSize,
                    Page = options.Page,
                }, cancellationToken: token));

            return result.Select(x => new Movie
            {
                Id = x.id,
                Title = x.title,
                YearOfRelease = x.yearofrelease,
                Rating = (float?)x.rating,
                UserRating = (int?)x.userrating,
                Genres = Enumerable.ToList(x.genres.Split(','))
            });
        }

        public async Task<Movie?> GetByIdAsync(Guid id, Guid? userId = default, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            var movie = await connection.QuerySingleOrDefaultAsync<Movie>(
                new CommandDefinition("""
                    Select m.*, round(avg(r.rating), 1) as rating, myr.rating as userrating
                    FROM movies m
                    LEFT JOIN ratings r ON m.id = r.movieid
                    LEFT JOIN ratings myr ON m.id = r.movieid AND myr.userid = @userId
                    WHERE id = @Id
                    group by m.id, m.title, m.yearofrelease, myr.rating, m.slug
                    """, new { id, userId }, cancellationToken: token
                ));

            if (movie == null)
            {
                return null;
            }

            var genres = await connection.QueryAsync<string>(
                new CommandDefinition("""
                    select name from genres where movieid = @id
                    """, new { id }, cancellationToken: token));

            foreach (var genre in genres)
            {
                movie.Genres.Add(genre);
            }

            return movie;
        }

        public async Task<Movie?> GetBySlugAsync(string slug, Guid? userId = default, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            var movie = await connection.QuerySingleOrDefaultAsync<Movie>(
                new CommandDefinition("""
                    Select m.*, round(avg(r.rating), 1) as rating, myr.rating as userrating
                    FROM movies m
                    LEFT JOIN ratings r ON m.id = r.movieid
                    LEFT JOIN ratings myr ON m.id = r.movieid AND myr.userid = @userId
                    WHERE slug = @Slug
                    group by m.id, m.title, m.yearofrelease, myr.rating, m.slug
                    """, new { slug, userId }, cancellationToken: token
                ));

            if (movie == null)
            {
                return null;
            }

            var genres = await connection.QueryAsync<string>(
                new CommandDefinition("""
                    select name from genres where movieid = @id
                    """, new { id = movie.Id }, cancellationToken: token));

            foreach (var genre in genres)
            {
                movie.Genres.Add(genre);
            }

            return movie;
        }

        public async Task<int> GetCountAsync(string? title, int? yearOfRelease, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();

            return await connection.QuerySingleAsync<int>(new CommandDefinition("""
                SELECT COUNT(id) FROM movies
                WHERE (@Title is null or title like '%' + @Title + '%')
                AND (@YearOfRelease is null or yearofrelease = @YearOfRelease)
                """, new { title, yearOfRelease }, cancellationToken: token));
        }

        public async Task<bool> UpdateAsync(Movie movie, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync();
            using var transaction = connection.BeginTransaction();

            await connection.ExecuteAsync(new CommandDefinition("""
                delete from genres where movieid = @id
                """, new { id = movie.Id }, transaction, cancellationToken: token));

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
                    """, movie, transaction, cancellationToken: token));

            transaction.Commit();
            return result > 0;
        }
    }
}
