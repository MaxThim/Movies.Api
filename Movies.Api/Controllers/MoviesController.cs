using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Movies.Api.Auth;
using Movies.Api.Mapping;
using Movies.Application.Models;
using Movies.Application.Services;
using Movies.Contracs.Requests;

namespace Movies.Api.Controllers
{
    [ApiController]
    public class MoviesController : ControllerBase
    {
        private readonly IMovieService _movieService;
        private readonly IRatingService _ratingService;

        public MoviesController(IMovieService movieService, IRatingService ratingService)
        {
            _movieService = movieService;
            _ratingService = ratingService;
        }

        [Authorize(AuthConstants.TrustMemberPolicyName)]
        [HttpPost(ApiEndpoints.Movies.Create)]
        public async Task<IActionResult> Create([FromBody] CreateMovieRequest request, CancellationToken token)
        {
            var movie = request.MapToMovie();
            await _movieService.CreateAsync(movie, token);
            var movieResponse = movie.MapToResponse();
            return CreatedAtAction(nameof(Get), new { idOrSlug = movie.Id }, movieResponse);
        }

        [HttpGet(ApiEndpoints.Movies.Get)]
        public async Task<IActionResult> Get([FromRoute] string idOrSlug, CancellationToken token)
        {
            var userId = HttpContext.GetUserId();

            var movie = Guid.TryParse(idOrSlug, out var id)
                ? await _movieService.GetByIdAsync(id, userId, token)
                : await _movieService.GetBySlugAsync(idOrSlug, userId, token);

            if (movie == null)
            {
                return NotFound();
            }

            var response = movie.MapToResponse();
            return Ok(response);
        }

        [HttpGet(ApiEndpoints.Movies.GetAll)]
        public async Task<IActionResult> GetAll([FromQuery] GetAllMoviesRequest request, CancellationToken token)
        {
            var userId = HttpContext.GetUserId();
            var options = request.MapToOptions()
                .WithUser(userId);
            var movies = await _movieService.GetAllAsync(options, token);
            var movieCount = await _movieService.GetCountAsync(options.Title, options.YearOfRelease, token);

            var moviesResponse = movies.MapToResponse(request.Page, request.PageSize, movieCount);
            return Ok(moviesResponse);
        }

        [Authorize(AuthConstants.TrustMemberPolicyName)]
        [HttpPut(ApiEndpoints.Movies.Update)]
        public async Task<IActionResult> Update([FromRoute] Guid id, [FromBody] UpdateMovieRequest request, CancellationToken token)
        {
            var movie = request.MapToMovie(id);
            var userId = HttpContext.GetUserId();

            var updatedMovie = await _movieService.UpdateAsync(movie, userId, token); 
            if (updatedMovie == null)
            {
                return NotFound();
            }

            var response = updatedMovie.MapToResponse();
            return Ok(response);
        }

        [Authorize(AuthConstants.AdminUserPolicyName)]
        [HttpDelete(ApiEndpoints.Movies.Delete)]
        public async Task<IActionResult> Delete([FromRoute] Guid id, CancellationToken token)
        {
            var deleted = await _movieService.DeleteByIdAsync(id, token);
            if (!deleted)
            {
                return NotFound();
            }
            return Ok();
        }

        [Authorize]
        [HttpDelete(ApiEndpoints.Movies.DeleteRating)]
        public async Task<IActionResult> DeleteRating([FromRoute] Guid id, CancellationToken token)
        {
            var userId = HttpContext.GetUserId();

            var result = await _ratingService.DeleteRatingsAsync(id, userId!.Value, token);

            return result ? Ok() : NotFound();
        }


    }
}
