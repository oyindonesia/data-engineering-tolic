COPY (
{query}
)
TO '{path}' (
    FORMAT {format},
    COMPRESSION zstd
);
