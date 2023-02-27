using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Movies.Contracs.Requests
{
    public class PagedRequest
    {
        public required int Page { get; init; } = 1;
        public required int PageSize { get; set; } = 10;
    }
}
