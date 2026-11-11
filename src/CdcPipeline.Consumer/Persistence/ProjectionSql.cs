using CdcPipeline.Core.Contracts;
using NpgsqlTypes;

namespace CdcPipeline.Consumer.Persistence;

/// <summary>
/// SQL for one entity contract, generated once. Identifiers come from the compiled-in contracts, never from event
/// data; every value is a parameter.
/// </summary>
internal sealed class ProjectionSql
{
    public ProjectionSql(EntityContract contract)
    {
        Contract = contract;
        var keys = contract.Key.Select(k => k.Name).ToArray();
        var fields = contract.Fields.Select(f => f.Name).ToArray();
        var conflict = $"(source, {string.Join(", ", keys)})";
        const string Guard = "WHERE (t.pos_major, t.pos_minor) < (excluded.pos_major, excluded.pos_minor)";
        const string Bookkeeping = "pos_major = excluded.pos_major, pos_minor = excluded.pos_minor, tx_id = excluded.tx_id, " +
            "apply_count = t.apply_count + 1, applied_at = clock_timestamp(), index_dirty = true";

        // $1 source, then keys, then fields, then extra, pos_major, pos_minor, tx_id.
        var upsertColumns = keys.Prepend("source").Concat(fields).Concat(["extra", "pos_major", "pos_minor", "tx_id"]).ToArray();
        var placeholders = string.Join(", ", upsertColumns.Select((_, i) => i == upsertColumns.Length - 4 ? $"${i + 1}::jsonb" : $"${i + 1}"));
        Upsert = $"""
            INSERT INTO {contract.ReadModelTable} AS t ({string.Join(", ", upsertColumns)}, deleted)
            VALUES ({placeholders}, false)
            ON CONFLICT {conflict} DO UPDATE SET
                {string.Join(", ", fields.Select(f => $"{f} = excluded.{f}"))},
                extra = excluded.extra, deleted = false, {Bookkeeping}
            {Guard}
            RETURNING 1
            """;

        var deleteColumns = keys.Prepend("source").Concat(["pos_major", "pos_minor", "tx_id"]).ToArray();
        Delete = $"""
            INSERT INTO {contract.ReadModelTable} AS t ({string.Join(", ", deleteColumns)}, deleted)
            VALUES ({string.Join(", ", deleteColumns.Select((_, i) => $"${i + 1}"))}, true)
            ON CONFLICT {conflict} DO UPDATE SET deleted = true, {Bookkeeping}
            {Guard}
            RETURNING 1
            """;

        var keyList = string.Join(", ", keys.Select(k => $"r.{k}"));
        PendingIndex = $"""
            SELECT r.source, {keyList}, {string.Join(", ", fields.Select(f => $"r.{f}"))}, r.extra::text, r.deleted, r.pos_major, r.pos_minor,
                   (r.tx_id IS NOT NULL AND tx.completed_at IS NULL) AS forced
            FROM {contract.ReadModelTable} r
            LEFT JOIN rm_tx tx ON tx.source = r.source AND tx.tx_id = r.tx_id
            WHERE r.index_dirty
              AND (r.tx_id IS NULL OR tx.completed_at IS NOT NULL OR r.applied_at < clock_timestamp() - make_interval(secs => $1))
            ORDER BY r.applied_at
            LIMIT $2
            """;

        // unnest arrays: source, key columns, pos_major, pos_minor.
        var keyArrays = contract.Key.Select((k, i) => $"${i + 2}::{ArrayType(k.Type)}");
        var unnestCols = string.Join(", ", keys.Select(k => $"k_{k}"));
        MarkIndexed = $"""
            UPDATE {contract.ReadModelTable} AS r SET index_dirty = false, indexed_at = clock_timestamp()
            FROM unnest($1::text[], {string.Join(", ", keyArrays)}, ${keys.Length + 2}::numeric[], ${keys.Length + 3}::numeric[])
                 AS u(source, {unnestCols}, pos_major, pos_minor)
            WHERE r.source = u.source AND {string.Join(" AND ", keys.Select(k => $"r.{k} = u.k_{k}"))}
              AND r.pos_major = u.pos_major AND r.pos_minor = u.pos_minor
            """;
    }

    public EntityContract Contract { get; }

    public string Upsert { get; }

    public string Delete { get; }

    /// <summary>Dirty rows whose source transaction is complete (or that waited longer than <c>$1</c> seconds).</summary>
    public string PendingIndex { get; }

    /// <summary>Clears the dirty flag only if the row still has the position that was pushed to the index.</summary>
    public string MarkIndexed { get; }

    public static NpgsqlDbType DbType(FieldType type) => type switch
    {
        FieldType.Int32 => NpgsqlDbType.Integer,
        FieldType.Int64 => NpgsqlDbType.Bigint,
        FieldType.Decimal => NpgsqlDbType.Numeric,
        FieldType.Text => NpgsqlDbType.Text,
        FieldType.Timestamp => NpgsqlDbType.TimestampTz,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };

    private static string ArrayType(FieldType type) => type switch
    {
        FieldType.Int32 => "integer[]",
        FieldType.Int64 => "bigint[]",
        FieldType.Text => "text[]",
        _ => throw new NotSupportedException($"Key columns of type {type} are not supported."),
    };
}
