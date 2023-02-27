using Dapper;
using Movies.Application.Database;
using Movies.Application.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Movies.Application.Repositories
{
    public class RatingRepository : IRatingRepository
    {
        private readonly IDbConnectionFactory _dbconnectionFactory;

        public RatingRepository(IDbConnectionFactory dbconnectionFactory)
        {
            _dbconnectionFactory = dbconnectionFactory;
        }

        public async Task<bool> DeleteRatingsAsync(Guid movieId, Guid userId, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync(token);

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                DELETE FROM ratings
                WHERE movieid = @movieId
                AND userid = @userId
                """, new { movieId, userId }, cancellationToken: token));

            return result > 0;
        }

        public async Task<float?> GetRatingAsync(Guid movieId, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync(token);

            return await connection.QuerySingleOrDefaultAsync<float?>(new CommandDefinition("""
                select round(avg(r.rating), 1) from ratings r
                where movieid = @moveiId
                """, new { movieId }, cancellationToken: token));
        }

        public async Task<(float? Rating, int? UserRating)> GetRatingAsync(Guid movieId, Guid userId, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync(token);

            return await connection.QuerySingleOrDefaultAsync<(float?, int?)>(new CommandDefinition("""
                select round(avg(rating), 1),
                (select TOP 1 rating from ratings
                where movieid = @movieId AND userid = @userId)
                from ratings
                where movieid = @movieId
                """, new { movieId }, cancellationToken: token));
        }

        public async Task<bool> RateMovieAsync(Guid movieId, int rating, Guid userId, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync(token);

            var result = await connection.ExecuteAsync(new CommandDefinition("""
                MERGE ratings AS target
                USING (SELECT @userId, @movieId, @rating) AS source(userid, movieid, rating)
                ON (target.userid = source.userid AND target.movieid = source.movieid)
                WHEN MATCHED THEN
                  UPDATE SET target.rating = source.rating
                WHEN NOT MATCHED THEN
                  INSERT (userid, movieid, rating) VALUES (source.userid, source.movieid, source.rating);
                """, new { movieId, rating, userId }));

            return result > 0;
        }

        public async Task<IEnumerable<MovieRating>> GetRatingsForUserAsync(Guid userId, CancellationToken token = default)
        {
            using var connection = await _dbconnectionFactory.CreateConnectionAsync(token);

            var result = await connection.QueryAsync<MovieRating>(new CommandDefinition("""
                SELECT r.movieid, r.rating, m.slug
                FROM ratings r
                INNER JOIN movies m
                ON m.id = r.movieid
                WHERE userid = @userId
                """, new { userId }, cancellationToken: token));

            return result;
        }
    }
}
