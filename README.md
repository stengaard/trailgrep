trailgrep
---------

A horribly simple utility to fetch all the files in S3 that contains a given
string.

    Usage of trailgrep:

    trailgrep [opts] <bucket> <search term>

    -concurrency int
        How many concurrent keys to look at (default 8)
    -prefix string
        Prefix to search under
    -region string
        AWS region to search in (default "eu-west-1")
