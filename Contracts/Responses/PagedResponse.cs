using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Movies.Contracs.Responses
{
    public class PagedResponse<TResponse>
    {
        public IEnumerable<TResponse> Items { get; init; } = Enumerable.Empty<TResponse>();
        public required int PageSize { get; init; }
        public required int Page { get; init; }
        public required int Total { get; init; }
        public bool HasNextPage => Total > (Page * PageSize);
    }
}
