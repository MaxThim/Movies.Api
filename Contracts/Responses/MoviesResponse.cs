using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Movies.Contracs.Responses
{
    public class MoviesResponse
    {
        public IEnumerable<MovieResponse> Items { get; init; } = Enumerable.Empty<MovieResponse>();
    }
}
