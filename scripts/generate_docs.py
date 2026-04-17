"""
Generate Antora documentation for the Evergreen ILS PostgreSQL database.

Usage:
    uv run python scripts/generate_docs.py [--dsn DSN] [--schema SCHEMA] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "modules" / "ROOT" / "pages"
NAV_FILE = BASE_DIR / "modules" / "ROOT" / "nav.adoc"
ATTACHMENTS_DIR = BASE_DIR / "modules" / "ROOT" / "attachments"
DBML_OUTPUT = BASE_DIR / "evergreen.dbml"
DBDOCS_URL = "https://dbdocs.io/eby/evergreen"

HUB_TABLE_THRESHOLD = 20  # flag tables with more incoming FKs as "hub" tables

SCHEMA_DESCRIPTIONS = {
    'acq': 'Acquisitions — purchase orders, invoicing, funding, and provider management.',
    'action': 'Circulation — checkouts, holds, transits, and patron actions.',
    'action_trigger': 'Automation/Events — event definitions, hooks, and reactor/validator configurations.',
    'actor': 'Users/Organizations — patrons, staff, org units, and user settings.',
    'asset': 'Items/Copies — copy management, call numbers, copy locations, and status.',
    'auditor': 'Audit Trails — historical snapshots of key tables (no foreign keys by design).',
    'authority': 'Authority Records — MARC authority control and heading management.',
    'biblio': 'Bibliographic Records — bib records, record entries, and TCN management.',
    'booking': 'Reservations — resource booking and reservation management.',
    'config': 'Configuration — system-wide configuration tables (largest schema).',
    'container': 'Buckets/Collections — patron and staff bucket groupings.',
    'evergreen': 'Core System — system-level functions (no tables).',
    'extend_reporter': 'Extended Reporting — supplemental reporter data.',
    'metabib': 'Search/Indexing — metabib field entries, search vectors, and facets.',
    'money': 'Financial — billings, payments, and transaction summaries.',
    'oai': 'OAI-PMH Protocol — Open Archives Initiative protocol support.',
    'offline': 'Offline Mode — offline circulation session staging.',
    'openapi': 'REST API — OpenAPI endpoint and parameter definitions.',
    'permission': 'Permissions — permission definitions, groups, and grant management.',
    'public': 'Public Schema — utility functions accessible without schema prefix.',
    'query': 'Query Builder — stored query/filter builder components.',
    'rating': 'Ratings — bibliographic record popularity rating.',
    'reporter': 'Reporting — reporter template and schedule management.',
    'search': 'Search Config — search filter configurations.',
    'serial': 'Serials — serial publication and issuance management.',
    'sip': 'SIP Protocol — SIP2 protocol configuration.',
    'staging': 'Import Staging — intermediate staging tables for bulk import.',
    'stats': 'Statistics — statistical tracking (no tables).',
    'unapi': 'Output Formats — UNAPI output format functions.',
    'url_verify': 'URL Verification — URL integrity checking.',
    'vandelay': 'Import/Export — batch import/export queue and processing.',
}

NAV_GROUPS = [
    ('Acquisitions', ['acq']),
    ('Circulation', ['action', 'action_trigger', 'booking']),
    ('Users & Organizations', ['actor', 'permission']),
    ('Items & Copies', ['asset']),
    ('Bibliographic', ['authority', 'biblio', 'serial']),
    ('Search & Indexing', ['metabib', 'search', 'rating']),
    ('Configuration', ['config']),
    ('Financial', ['money']),
    ('Reporting', ['reporter', 'extend_reporter', 'stats']),
    ('Import/Export', ['vandelay', 'staging']),
    ('Collections', ['container']),
    ('REST & Protocols', ['openapi', 'unapi', 'oai', 'sip']),
    ('System', ['evergreen', 'public', 'query', 'auditor']),
    ('Other', ['offline', 'url_verify']),
]

VOLATILITY_LABELS = {'i': 'IMMUTABLE', 's': 'STABLE', 'v': 'VOLATILE'}
CASCADE_LABELS = {'a': 'NO ACTION', 'r': 'RESTRICT', 'c': 'CASCADE', 'n': 'SET NULL', 'd': 'SET DEFAULT'}

# Tables that write to other tables (UPDATE/INSERT INTO) inside trigger bodies
_WRITE_STMT_RE = re.compile(
    r'\b(UPDATE|INSERT\s+INTO)\s+(?:ONLY\s+)?(?:([a-z_]\w*)\.)?([a-z_]\w*)\b',
    re.IGNORECASE,
)
_SKIP_TABLE_WORDS = frozenset([
    'set', 'only', 'table', 'from', 'where', 'returning', 'select', 'into',
    'values', 'default', 'null', 'true', 'false', 'new', 'old', 'the', 'a',
    'an', 'is', 'or', 'and', 'not', 'if', 'then', 'else', 'end', 'begin',
    'declare', 'return', 'perform', 'raise', 'loop', 'for', 'while', 'do',
    'each', 'row', 'statement', 'with', 'as', 'in', 'on', 'by', 'to', 'of',
    'at', 'be', 'it', 'that', 'this', 'will', 'all', 'any', 'both', 'peer',
    'found', 'coalesce', 'case', 'when', 'join', 'inner', 'outer', 'left',
    'right', 'cross', 'using', 'except', 'union', 'intersect', 'limit',
    'offset', 'order', 'group', 'having', 'distinct', 'exists', 'between',
    'like', 'ilike', 'similar', 'some', 'array', 'row',
])
_COMMENT_RE = re.compile(r'--[^\n]*|/\*.*?\*/', re.DOTALL)
_INDEX_METHOD_RE = re.compile(r'\bUSING\s+(\w+)', re.IGNORECASE)


# ---------------------------------------------------------------------------
# Database introspection
# ---------------------------------------------------------------------------

class DatabaseIntrospector:
    def __init__(self, conn):
        self.conn = conn

    def _cur(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def get_schemas(self) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    n.nspname AS schema_name,
                    COUNT(DISTINCT t.oid) FILTER (WHERE t.relkind = 'r') AS table_count,
                    COUNT(DISTINCT v.oid) FILTER (WHERE v.relkind = 'v') AS view_count,
                    COUNT(DISTINCT p.oid) AS function_count
                FROM pg_namespace n
                LEFT JOIN pg_class t ON t.relnamespace = n.oid AND t.relkind = 'r'
                LEFT JOIN pg_class v ON v.relnamespace = n.oid AND v.relkind = 'v'
                LEFT JOIN pg_proc p ON p.pronamespace = n.oid
                WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                  AND n.nspname NOT LIKE 'pg_temp_%'
                  AND n.nspname NOT LIKE 'pg_toast_temp_%'
                GROUP BY n.nspname
                ORDER BY n.nspname
            """)
            return [dict(r) for r in cur.fetchall()]

    def get_tables(self, schema: str) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    c.relname AS table_name,
                    obj_description(c.oid, 'pg_class') AS table_comment,
                    greatest(c.reltuples::bigint, 0) AS row_estimate,
                    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                    c.oid AS relid
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s AND c.relkind = 'r'
                ORDER BY c.relname
            """, (schema,))
            return [dict(r) for r in cur.fetchall()]

    def get_views(self, schema: str) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    c.relname AS view_name,
                    obj_description(c.oid, 'pg_class') AS view_comment,
                    pg_get_viewdef(c.oid, true) AS definition,
                    c.oid AS relid
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = %s AND c.relkind = 'v'
                ORDER BY c.relname
            """, (schema,))
            return [dict(r) for r in cur.fetchall()]

    def get_view_columns(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    a.attnum AS ordinal,
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    NOT a.attnotnull AS is_nullable,
                    col_description(a.attrelid, a.attnum) AS column_comment
                FROM pg_attribute a
                WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_columns(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    a.attnum AS ordinal,
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    NOT a.attnotnull AS is_nullable,
                    pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
                    col_description(a.attrelid, a.attnum) AS column_comment
                FROM pg_attribute a
                LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
                WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_primary_key(self, relid: int) -> list[str]:
        with self._cur() as cur:
            cur.execute("""
                SELECT a.attname
                FROM pg_index ix
                JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
                WHERE ix.indrelid = %s AND ix.indisprimary
                ORDER BY array_position(ix.indkey, a.attnum)
            """, (relid,))
            return [r['attname'] for r in cur.fetchall()]

    def get_foreign_keys(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    c.conname AS constraint_name,
                    array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS local_columns,
                    fn.nspname AS foreign_schema,
                    fc.relname AS foreign_table,
                    array_agg(fa.attname ORDER BY array_position(c.confkey, fa.attnum)) AS foreign_columns,
                    c.confdeltype AS delete_action,
                    c.confupdtype AS update_action,
                    c.condeferrable AS deferrable,
                    c.condeferred AS initially_deferred
                FROM pg_constraint c
                JOIN pg_class fc ON c.confrelid = fc.oid
                JOIN pg_namespace fn ON fc.relnamespace = fn.oid
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                JOIN pg_attribute fa ON fa.attrelid = c.confrelid AND fa.attnum = ANY(c.confkey)
                WHERE c.contype = 'f' AND c.conrelid = %s
                GROUP BY c.conname, fn.nspname, fc.relname, c.confdeltype,
                         c.confupdtype, c.condeferrable, c.condeferred, c.conkey, c.confkey
                ORDER BY c.conname
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_incoming_fks(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT DISTINCT
                    rn.nspname AS referencing_schema,
                    rc.relname AS referencing_table,
                    array_agg(a.attname ORDER BY array_position(cn.conkey, a.attnum)) AS referencing_columns,
                    array_agg(fa.attname ORDER BY array_position(cn.confkey, fa.attnum)) AS referenced_columns,
                    cn.conname AS constraint_name
                FROM pg_constraint cn
                JOIN pg_class rc ON cn.conrelid = rc.oid
                JOIN pg_namespace rn ON rc.relnamespace = rn.oid
                JOIN pg_attribute a ON a.attrelid = cn.conrelid AND a.attnum = ANY(cn.conkey)
                JOIN pg_attribute fa ON fa.attrelid = cn.confrelid AND fa.attnum = ANY(cn.confkey)
                WHERE cn.contype = 'f' AND cn.confrelid = %s
                GROUP BY rn.nspname, rc.relname, cn.conname
                ORDER BY rn.nspname, rc.relname
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_global_incoming_fk_counts(self) -> dict[tuple[str, str], int]:
        """Returns {(schema, table): incoming_fk_count} for all tables."""
        with self._cur() as cur:
            cur.execute("""
                SELECT fn.nspname AS schema, fc.relname AS table_name, COUNT(*) AS cnt
                FROM pg_constraint c
                JOIN pg_class fc ON c.confrelid = fc.oid
                JOIN pg_namespace fn ON fc.relnamespace = fn.oid
                WHERE c.contype = 'f'
                GROUP BY fn.nspname, fc.relname
            """)
            return {(r['schema'], r['table_name']): r['cnt'] for r in cur.fetchall()}

    def get_indexes(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    i.relname AS index_name,
                    ix.indisunique AS is_unique,
                    ix.indisprimary AS is_primary,
                    pg_get_indexdef(ix.indexrelid) AS index_definition
                FROM pg_index ix
                JOIN pg_class i ON i.oid = ix.indexrelid
                WHERE ix.indrelid = %s
                ORDER BY ix.indisprimary DESC, ix.indisunique DESC, i.relname
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_triggers(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    t.tgname AS trigger_name,
                    t.tgtype AS tgtype,
                    p.proname AS function_name,
                    n.nspname AS function_schema,
                    l.lanname AS language,
                    p.prosrc AS function_body,
                    pg_get_triggerdef(t.oid) AS trigger_definition
                FROM pg_trigger t
                JOIN pg_proc p ON t.tgfoid = p.oid
                JOIN pg_namespace n ON p.pronamespace = n.oid
                JOIN pg_language l ON p.prolang = l.oid
                WHERE t.tgrelid = %s AND NOT t.tgisinternal
                ORDER BY t.tgname
            """, (relid,))
            rows = [dict(r) for r in cur.fetchall()]
            for r in rows:
                tgtype = r['tgtype']
                r['timing'] = 'BEFORE' if (tgtype & 2) else 'AFTER'
                r['level'] = 'ROW' if (tgtype & 1) else 'STATEMENT'
                events = []
                if tgtype & 4:
                    events.append('INSERT')
                if tgtype & 8:
                    events.append('DELETE')
                if tgtype & 16:
                    events.append('UPDATE')
                if tgtype & 32:
                    events.append('TRUNCATE')
                r['events'] = events
            return rows

    def get_check_constraints(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT c.conname AS constraint_name,
                       pg_get_constraintdef(c.oid) AS constraint_definition
                FROM pg_constraint c
                WHERE c.conrelid = %s AND c.contype = 'c'
                ORDER BY c.conname
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_unique_constraints(self, relid: int) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT c.conname AS constraint_name,
                       array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS columns
                FROM pg_constraint c
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                WHERE c.conrelid = %s AND c.contype = 'u'
                GROUP BY c.conname
                ORDER BY c.conname
            """, (relid,))
            return [dict(r) for r in cur.fetchall()]

    def get_functions(self, schema: str) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT
                    p.proname AS function_name,
                    pg_get_function_arguments(p.oid) AS arguments,
                    pg_get_function_result(p.oid) AS return_type,
                    l.lanname AS language,
                    p.prosrc AS source_body,
                    p.provolatile AS volatility,
                    p.proisstrict AS is_strict,
                    p.prosecdef AS security_definer,
                    p.proretset AS returns_set,
                    obj_description(p.oid, 'pg_proc') AS function_comment
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                JOIN pg_language l ON p.prolang = l.oid
                WHERE n.nspname = %s
                ORDER BY p.proname, pg_get_function_arguments(p.oid)
            """, (schema,))
            return [dict(r) for r in cur.fetchall()]

    def get_enums(self, schema: str) -> list[dict]:
        with self._cur() as cur:
            cur.execute("""
                SELECT t.typname AS type_name,
                       array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values,
                       obj_description(t.oid, 'pg_type') AS type_comment
                FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                JOIN pg_enum e ON e.enumtypid = t.oid
                WHERE n.nspname = %s
                GROUP BY t.typname, t.oid
                ORDER BY t.typname
            """, (schema,))
            return [dict(r) for r in cur.fetchall()]

    def get_schema_intra_fks(self, schema: str) -> list[dict]:
        """FK relationships where both sides are in the same schema. Used for ER diagrams."""
        with self._cur() as cur:
            cur.execute("""
                SELECT DISTINCT
                    lc.relname AS local_table,
                    fc.relname AS foreign_table,
                    array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)) AS columns
                FROM pg_constraint c
                JOIN pg_class lc ON c.conrelid = lc.oid
                JOIN pg_namespace ln ON lc.relnamespace = ln.oid
                JOIN pg_class fc ON c.confrelid = fc.oid
                JOIN pg_namespace fn ON fc.relnamespace = fn.oid
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
                WHERE c.contype = 'f' AND ln.nspname = %s AND fn.nspname = %s
                GROUP BY lc.relname, fc.relname, c.conname
                ORDER BY lc.relname, fc.relname
            """, (schema, schema))
            return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def esc(text) -> str:
    if text is None:
        return ''
    return str(text).replace('|', '\\|')


def xref_table(schema: str, table: str, label: str = None) -> str:
    page = f"tables/{schema}/{schema}_{table}.adoc"
    label = label or f"{schema}.{table}"
    return f"xref:{page}[{label}]"


def xref_view(schema: str, view: str, label: str = None) -> str:
    page = f"views/{schema}/{schema}_{view}.adoc"
    label = label or f"{schema}.{view}"
    return f"xref:{page}[{label}]"


def xref_schema(schema: str) -> str:
    return f"xref:schemas/{schema}.adoc[{schema}]"


def xref_function(func_schema: str, func_name: str, schemas_with_functions: set[str]) -> str:
    if func_schema in schemas_with_functions:
        return f"xref:functions/{func_schema}.adoc#{func_name}[`{func_schema}.{func_name}()`]"
    return f"`{func_schema}.{func_name}()`"


def index_method(indexdef: str) -> str:
    m = _INDEX_METHOD_RE.search(indexdef or '')
    return m.group(1).lower() if m else 'btree'


def is_data_modifying_trigger(trigger: dict) -> bool:
    if trigger['timing'] != 'BEFORE' or trigger['level'] != 'ROW':
        return False
    body = trigger['function_body'] or ''
    lang = trigger['language']
    if lang in ('plpgsql', 'sql'):
        return 'RETURN NEW' in body.upper() and (
            'NEW.' in body or ':=' in body
        )
    if lang in ('plperlu', 'plperl'):
        return 'return "MODIFY"' in body or "return 'MODIFY'" in body
    return False


def detect_side_effects(triggers: list[dict], current_schema: str,
                        current_table: str) -> list[dict]:
    """
    Scan AFTER trigger bodies for UPDATE/INSERT INTO statements targeting
    other tables. Returns list of {trigger_name, op, target_schema, target_table}.
    """
    results = []
    seen = set()
    for trig in triggers:
        raw_body = trig.get('function_body') or ''
        # Strip SQL/PL comments before parsing to avoid matching comment text
        body = _COMMENT_RE.sub(' ', raw_body)
        for m in _WRITE_STMT_RE.finditer(body):
            op_raw = m.group(1).upper().replace('\n', ' ').replace('  ', ' ')
            op = 'INSERT' if 'INSERT' in op_raw else 'UPDATE'
            target_schema = (m.group(2) or current_schema).lower()
            target_table = m.group(3).lower()
            if target_table in _SKIP_TABLE_WORDS:
                continue
            # Require plausible table name: >= 4 chars, and contains underscore or >= 6 chars
            if len(target_table) < 4:
                continue
            if len(target_table) < 6 and '_' not in target_table:
                continue
            if target_schema == current_schema and target_table == current_table:
                continue
            key = (target_schema, target_table)
            if key not in seen:
                seen.add(key)
                results.append({
                    'trigger_name': trig['trigger_name'],
                    'op': op,
                    'target_schema': target_schema,
                    'target_table': target_table,
                })
    return results


def source_lang(language: str) -> str:
    return {'plpgsql': 'plpgsql', 'sql': 'sql',
            'plperlu': 'perl', 'plperl': 'perl'}.get(language, language)




# ---------------------------------------------------------------------------
# Page renderers
# ---------------------------------------------------------------------------

def render_home(schemas: list[dict], gen_date: str) -> str:
    total_tables = sum(s['table_count'] for s in schemas)
    total_views = sum(s['view_count'] for s in schemas)
    total_functions = sum(s['function_count'] for s in schemas)

    lines = [
        "= Evergreen Database Reference",
        ":description: Auto-generated reference for the Evergreen ILS PostgreSQL database.",
        "",
        f"Complete reference for the Evergreen ILS PostgreSQL database: "
        f"{total_tables} tables, {total_views} views, and {total_functions} functions across {len(schemas)} schemas.",
        "",
        f"_Generated {gen_date}. Do not edit — regenerate with `make generate`._"
        f" Also available as an interactive diagram on link:{DBDOCS_URL}[dbdocs.io^]"
        f" and as a link:{{attachmentsdir}}/evergreen.dbml[downloadable DBML schema file].",
        "",
        "* xref:write-safety.adoc[Write Safety Reference] — all tables with data-modifying triggers",
        "",
        "== Schemas",
        "",
        '[cols="2,5,1,1,1"]',
        "|===",
        "|Schema |Purpose |Tables |Views |Functions",
        "",
    ]
    for s in schemas:
        name = s['schema_name']
        desc = SCHEMA_DESCRIPTIONS.get(name, '')
        lines.append(f"| {xref_schema(name)}")
        lines.append(f"| {esc(desc)}")
        lines.append(f"| {s['table_count']}")
        lines.append(f"| {s['view_count']}")
        lines.append(f"| {s['function_count']}")
        lines.append("")
    lines.append("|===")
    return '\n'.join(lines) + '\n'


def render_schema_index(schema_info: dict, tables: list[dict], views: list[dict],
                        enums: list[dict], trigger_counts: dict, fk_counts: dict) -> str:
    name = schema_info['schema_name']
    desc = SCHEMA_DESCRIPTIONS.get(name, f'The `{name}` schema.')
    table_count = schema_info['table_count']
    view_count = schema_info['view_count']
    func_count = schema_info['function_count']
    total_triggers = sum(trigger_counts.values())

    lines = [
        f"= Schema: {name}",
        f":description: {desc}",
        "",
        desc,
        "",
        '[cols="1,1"]',
        "|===",
        f"|Tables |{table_count}",
        f"|Views |{view_count}",
        f"|Functions |{func_count}",
        f"|Triggers |{total_triggers}",
        f"|Enum Types |{len(enums)}",
        "|===",
        "",
    ]

    if total_triggers > 10:
        lines += [
            "[WARNING]",
            "====",
            f"This schema has *{total_triggers} triggers*. Many triggers modify data before write "
            "(BEFORE ROW triggers) or maintain denormalized summary tables (AFTER triggers). "
            "Review trigger bodies carefully before writing to these tables directly.",
            "====",
            "",
        ]

    if table_count > 0:
        lines += [
            "== Tables",
            "",
            '[cols="3,1,1,5"]',
            "|===",
            "|Table |Triggers |FKs |Description",
            "",
        ]
        for t in tables:
            tname = t['table_name']
            trig_n = trigger_counts.get(t['relid'], 0)
            fk_n = fk_counts.get(t['relid'], 0)
            comment_first = ''
            if t['table_comment']:
                comment_first = t['table_comment'].split('\n')[0]
            trig_badge = f" ⚡ {trig_n}" if trig_n > 0 else ''
            lines.append(f"| {xref_table(name, tname, tname)}{trig_badge}")
            lines.append(f"| {trig_n}")
            lines.append(f"| {fk_n}")
            lines.append(f"| {esc(comment_first)}")
            lines.append("")
        lines.append("|===")
        lines.append("")

    if view_count > 0:
        lines += [
            "== Views",
            "",
            '[cols="3,6"]',
            "|===",
            "|View |Description",
            "",
        ]
        for v in views:
            vname = v['view_name']
            comment_first = ''
            if v['view_comment']:
                comment_first = v['view_comment'].split('\n')[0]
            lines.append(f"| {xref_view(name, vname, vname)}")
            lines.append(f"| {esc(comment_first)}")
            lines.append("")
        lines.append("|===")
        lines.append("")

    if enums:
        lines += [
            "== Types",
            "",
            f"See xref:types/{name}.adoc[{name} Types] for enum type definitions.",
            "",
        ]

    if func_count > 0:
        lines += [
            "== Functions",
            "",
            f"See xref:functions/{name}.adoc[{name} Functions ({func_count})].",
            "",
        ]

    return '\n'.join(lines) + '\n'


def render_table_page(schema: str, table: str, table_info: dict,
                      columns: list, primary_key: list,
                      foreign_keys: list, incoming_fks: list,
                      indexes: list, triggers: list,
                      check_constraints: list, unique_constraints: list,
                      incoming_fk_count: int,
                      schemas_with_functions: set[str]) -> str:
    pk_set = set(primary_key)
    fk_col_map = {}
    for fk in foreign_keys:
        for col in (fk['local_columns'] or []):
            fk_col_map[col] = fk

    has_modifying_triggers = any(is_data_modifying_trigger(t) for t in triggers)
    cascade_delete_fks = [fk for fk in foreign_keys if fk['delete_action'] == 'c']
    deferrable_fks = [fk for fk in foreign_keys if fk['deferrable']]
    soft_delete = any(
        c['column_name'] == 'deleted' and 'bool' in (c['data_type'] or '')
        for c in columns
    )
    side_effects = detect_side_effects(triggers, schema, table)
    is_hub = incoming_fk_count > HUB_TABLE_THRESHOLD

    lines = [
        f"= {schema}.{table}",
        f":description: Table reference for {schema}.{table}",
        "",
        f"xref:schemas/{schema}.adoc[↑ {schema} schema]",
        "",
    ]

    if table_info.get('table_comment'):
        lines += [table_info['table_comment'], ""]

    if is_hub:
        lines += [
            "[NOTE]",
            "====",
            f"*Hub Table:* This table is referenced by *{incoming_fk_count} foreign keys* across the database. "
            "It is a central structural table — changes to rows here have wide-reaching effects. "
            "Consider all dependent schemas before deleting or modifying rows.",
            "====",
            "",
        ]

    if soft_delete:
        lines += [
            "[WARNING]",
            "====",
            "*Soft Deletes:* This table uses a `deleted` boolean flag rather than physical row deletion. "
            "Ad-hoc queries **must** include `WHERE deleted = false` to exclude logically-deleted rows. "
            "The application layer enforces this automatically; direct SQL does not.",
            "====",
            "",
        ]

    if has_modifying_triggers:
        lines += [
            "[CAUTION]",
            "====",
            "*Data-Modifying Triggers:* This table has BEFORE ROW trigger(s) that modify row data before write. "
            "Values you INSERT or UPDATE may differ from what is actually stored. "
            "See the <<triggers,Triggers>> section below.",
            "====",
            "",
        ]

    if cascade_delete_fks:
        targets = ', '.join(
            xref_table(fk['foreign_schema'], fk['foreign_table'])
            for fk in cascade_delete_fks
        )
        lines += [
            "[WARNING]",
            "====",
            f"*Cascading Deletes:* Deleting rows from this table will cascade to: {targets}.",
            "====",
            "",
        ]

    if deferrable_fks:
        names = ', '.join(f"`{fk['constraint_name']}`" for fk in deferrable_fks)
        lines += [
            "[WARNING]",
            "====",
            f"*Deferrable Constraints:* The following FK constraints are deferrable — "
            f"they are checked at transaction end, not statement end: {names}.",
            "====",
            "",
        ]

    if side_effects:
        lines += [
            "[NOTE]",
            "====",
            "*Trigger Side Effects:* Writing to this table automatically triggers writes to other tables:",
            "",
        ]
        for se in side_effects:
            lines.append(
                f"* `{se['trigger_name']}` → {se['op']} "
                f"{xref_table(se['target_schema'], se['target_table'])}"
            )
        lines += ["====", ""]

    # Columns
    lines += [
        "== Columns",
        "",
        '[cols="2,2,1,3,4"]',
        "|===",
        "|Column |Type |Nullable |Default |Notes",
        "",
    ]
    for col in columns:
        cname = col['column_name']
        badges = []
        if cname in pk_set:
            badges.append('[.label]#PK#')
        if cname in fk_col_map:
            badges.append('[.label]#FK#')
        if cname == 'deleted' and 'bool' in (col['data_type'] or ''):
            badges.append('[.label.warning]#SOFT-DEL#')
        badge_str = ' '.join(badges)
        label = f"*{cname}*" if cname in pk_set else cname
        if badge_str:
            label = f"{label} {badge_str}"

        fk_note = ''
        if cname in fk_col_map:
            fk = fk_col_map[cname]
            fcols = fk['foreign_columns'] or []
            fk_label = "{}.{}({})".format(fk['foreign_schema'], fk['foreign_table'], ', '.join(fcols))
            fk_note = f"→ {xref_table(fk['foreign_schema'], fk['foreign_table'], fk_label)}"

        comment = esc(col['column_comment'] or '')
        notes = ' '.join(filter(None, [comment, fk_note]))

        lines.append(f"| {esc(label)}")
        lines.append(f"| `{esc(col['data_type'])}`")
        lines.append(f"| {'Yes' if col['is_nullable'] else 'No'}")
        lines.append(f"| {esc(col['column_default'] or '')}")
        lines.append(f"| {notes}")
        lines.append("")
    lines.append("|===")
    lines.append("")

    if primary_key:
        lines += ["== Primary Key", "", f"`({', '.join(primary_key)})`", ""]

    if foreign_keys:
        lines += [
            "== Foreign Keys",
            "",
            '[cols="2,3,1,1,1,2"]',
            "|===",
            "|Column(s) |References |On Delete |On Update |Deferrable |Constraint",
            "",
        ]
        for fk in foreign_keys:
            local_cols = ', '.join(fk['local_columns'] or [])
            foreign_cols = ', '.join(fk['foreign_columns'] or [])
            ref_label = f"{fk['foreign_schema']}.{fk['foreign_table']}({foreign_cols})"
            del_action = CASCADE_LABELS.get(fk['delete_action'], fk['delete_action'] or '')
            upd_action = CASCADE_LABELS.get(fk['update_action'], fk['update_action'] or '')
            if fk['delete_action'] == 'c':
                del_action = '[.label]#CASCADE#'
            defer = 'Yes' if fk['deferrable'] else 'No'
            if fk['deferrable'] and fk['initially_deferred']:
                defer = 'DEFERRED'
            lines.append(f"| `{esc(local_cols)}`")
            lines.append(f"| {xref_table(fk['foreign_schema'], fk['foreign_table'], ref_label)}")
            lines.append(f"| {del_action}")
            lines.append(f"| {esc(upd_action)}")
            lines.append(f"| {defer}")
            lines.append(f"| `{esc(fk['constraint_name'])}`")
            lines.append("")
        lines.append("|===")
        lines.append("")

    if unique_constraints:
        lines += ["== Unique Constraints", ""]
        for uc in unique_constraints:
            cols = ', '.join(uc['columns'] or [])
            lines.append(f"* `{esc(uc['constraint_name'])}`: `({esc(cols)})`")
        lines.append("")

    if check_constraints:
        lines += ["== Check Constraints", ""]
        for cc in check_constraints:
            lines.append(f"* `{esc(cc['constraint_name'])}`: `{esc(cc['constraint_definition'])}`")
        lines.append("")

    if indexes:
        lines += [
            "== Indexes",
            "",
            '[cols="3,1,5"]',
            "|===",
            "|Index |Method |Definition",
            "",
        ]
        for idx in indexes:
            badges = []
            if idx['is_primary']:
                badges.append('[.label]#PK#')
            elif idx['is_unique']:
                badges.append('[.label]#UNIQUE#')
            badge_str = ' '.join(badges)
            name_label = f"`{esc(idx['index_name'])}`"
            if badge_str:
                name_label = f"{name_label} {badge_str}"
            method = index_method(idx['index_definition'])
            method_label = f"[.label]#GIN#" if method == 'gin' else (
                           f"[.label]#GiST#" if method == 'gist' else method)
            lines.append(f"| {name_label}")
            lines.append(f"| {method_label}")
            lines.append(f"| `{esc(idx['index_definition'])}`")
            lines.append("")
        lines.append("|===")
        lines.append("")

    if triggers:
        lines += [
            "[[triggers]]",
            "== Triggers",
            "",
            '[cols="3,1,2,1,3"]',
            "|===",
            "|Trigger |Timing |Event |Level |Function",
            "",
        ]
        for trig in triggers:
            timing_label = "[.label]#BEFORE#" if trig['timing'] == 'BEFORE' else 'AFTER'
            event_str = ' OR '.join(trig['events'])
            func_ref = xref_function(trig['function_schema'], trig['function_name'],
                                     schemas_with_functions)
            lines.append(f"| `{esc(trig['trigger_name'])}`")
            lines.append(f"| {timing_label}")
            lines.append(f"| {esc(event_str)}")
            lines.append(f"| {trig['level']}")
            lines.append(f"| {func_ref}")
            lines.append("")
        lines.append("|===")
        lines.append("")

        lines.append("=== Trigger Bodies")
        lines.append("")
        for trig in triggers:
            lang = trig['language']
            body = trig['function_body'] or ''
            lines.append(f"==== {trig['trigger_name']}")
            lines.append("")
            lines.append(f"*Function:* {xref_function(trig['function_schema'], trig['function_name'], schemas_with_functions)} +")
            lines.append(f"*Timing:* {trig['timing']} {' OR '.join(trig['events'])} {trig['level']}")
            lines.append("")
            if is_data_modifying_trigger(trig):
                lines += [
                    "[CAUTION]",
                    "====",
                    "This trigger modifies the row before it is written (returns a modified `NEW`).",
                    "====",
                    "",
                ]
            if lang in ('c', 'internal'):
                lines.append("_C language extension — see the Evergreen source code._")
            else:
                lines.append(f"[source,{source_lang(lang)}]")
                lines.append("----")
                lines.append(body.rstrip())
                lines.append("----")
            lines.append("")

    if incoming_fks:
        lines += [
            "== Referenced By",
            "",
            f"The following tables have foreign keys pointing to `{schema}.{table}` "
            f"({len(incoming_fks)} referencing table(s)):",
            "",
            '[cols="3,2,2,2"]',
            "|===",
            "|Table |Referencing Column(s) |Referenced Column(s) |Constraint",
            "",
        ]
        for ifk in incoming_fks:
            ref_cols = ', '.join(ifk['referencing_columns'] or [])
            src_cols = ', '.join(ifk['referenced_columns'] or [])
            lines.append(f"| {xref_table(ifk['referencing_schema'], ifk['referencing_table'])}")
            lines.append(f"| `{esc(ref_cols)}`")
            lines.append(f"| `{esc(src_cols)}`")
            lines.append(f"| `{esc(ifk['constraint_name'])}`")
            lines.append("")
        lines.append("|===")
        lines.append("")

    return '\n'.join(lines) + '\n'


def render_view_page(schema: str, view: str, view_info: dict, columns: list) -> str:
    lines = [
        f"= {schema}.{view} (view)",
        f":description: View reference for {schema}.{view}",
        "",
        f"xref:schemas/{schema}.adoc[↑ {schema} schema]",
        "",
        "[NOTE]",
        "====",
        "This is a *database view*, not a base table. It has no triggers, indexes, or FK constraints of its own. "
        "Querying this view may be more efficient than joining the underlying tables directly.",
        "====",
        "",
    ]

    if view_info.get('view_comment'):
        lines += [view_info['view_comment'], ""]

    lines += [
        "== Columns",
        "",
        '[cols="2,2,1,4"]',
        "|===",
        "|Column |Type |Nullable |Notes",
        "",
    ]
    for col in columns:
        lines.append(f"| {esc(col['column_name'])}")
        lines.append(f"| `{esc(col['data_type'])}`")
        lines.append(f"| {'Yes' if col['is_nullable'] else 'No'}")
        lines.append(f"| {esc(col['column_comment'] or '')}")
        lines.append("")
    lines.append("|===")
    lines.append("")

    if view_info.get('definition'):
        lines += [
            "== View Definition",
            "",
            "[source,sql]",
            "----",
            (view_info['definition'] or '').rstrip(),
            "----",
            "",
        ]

    return '\n'.join(lines) + '\n'


def render_types_page(schema: str, enums: list[dict]) -> str:
    lines = [
        f"= {schema} Types",
        f":description: Enum and domain type reference for the {schema} schema.",
        "",
        f"xref:schemas/{schema}.adoc[↑ {schema} schema]",
        "",
        f"This page documents all {len(enums)} enum type(s) defined in the `{schema}` schema.",
        "",
        "[NOTE]",
        "====",
        "Columns using these types only accept the listed values. "
        "Inserting any other value will raise a `invalid input value for enum` error.",
        "====",
        "",
    ]

    for enum in enums:
        name = enum['type_name']
        values = enum['values'] or []
        lines.append(f"== {name}")
        lines.append("")
        if enum['type_comment']:
            lines += [esc(enum['type_comment']), ""]
        lines.append(f"*Type:* `{schema}.{name}`")
        lines.append("")
        lines += [
            '[cols="1,4"]',
            "|===",
            "|Value |Notes",
            "",
        ]
        for v in values:
            lines.append(f"| `{esc(v)}`")
            lines.append("|")
            lines.append("")
        lines.append("|===")
        lines.append("")

    return '\n'.join(lines) + '\n'


def render_functions_page(schema: str, functions: list[dict]) -> str:
    lines = [
        f"= {schema} Functions",
        f":description: Function reference for the {schema} schema.",
        "",
        f"xref:schemas/{schema}.adoc[↑ {schema} schema]",
        "",
        f"This page documents all {len(functions)} function(s) in the `{schema}` schema.",
        "",
    ]

    name_counts: dict[str, int] = defaultdict(int)
    for f in functions:
        name_counts[f['function_name']] += 1

    # Index table
    lines += [
        "== Function Index",
        "",
        '[cols="4,3,1,1,1"]',
        "|===",
        "|Function |Return Type |Language |Volatility |Security",
        "",
    ]
    anchor_counters: dict[str, int] = defaultdict(int)
    for f in functions:
        fname = f['function_name']
        anchor_counters[fname] += 1
        idx = anchor_counters[fname]
        anchor = fname if name_counts[fname] == 1 else f"{fname}_{idx}"
        short_args = f['arguments']
        if len(short_args) > 40:
            short_args = short_args[:37] + '...'
        vol = VOLATILITY_LABELS.get(f['volatility'], f['volatility'] or '')
        sec = '[.label]#SECDEF#' if f['security_definer'] else ''
        lines.append(f"| <<{anchor},{esc(fname)}({esc(short_args)})>>")
        lines.append(f"| `{esc(f['return_type'])}`")
        lines.append(f"| {esc(f['language'])}")
        lines.append(f"| {vol}")
        lines.append(f"| {sec}")
        lines.append("")
    lines.append("|===")
    lines.append("")

    # Per-function sections
    anchor_counters2: dict[str, int] = defaultdict(int)
    for f in functions:
        fname = f['function_name']
        anchor_counters2[fname] += 1
        idx = anchor_counters2[fname]
        anchor = fname if name_counts[fname] == 1 else f"{fname}_{idx}"
        vol = VOLATILITY_LABELS.get(f['volatility'], f['volatility'] or '')
        lang = f['language']
        body = f['source_body'] or ''

        lines.append(f"[[{anchor}]]")
        lines.append(f"=== {fname}")
        lines.append("")
        lines.append(f"*Signature:* `{schema}.{fname}({f['arguments']})`")
        lines.append("")
        lines.append(f"*Returns:* `{esc(f['return_type'])}`")
        lines.append("")
        lines += [
            '[cols="1,2"]',
            "|===",
            f"|Language |{esc(lang)}",
            f"|Volatility |{vol}",
            f"|Strict |{'Yes (returns NULL on NULL input)' if f['is_strict'] else 'No'}",
            f"|Security Definer |{'Yes' if f['security_definer'] else 'No'}",
            "|===",
            "",
        ]

        if f['function_comment']:
            lines += [esc(f['function_comment']), ""]

        if f['security_definer']:
            lines += [
                "[WARNING]",
                "====",
                "This function runs with the privileges of its *owner*, not the calling user (SECURITY DEFINER).",
                "====",
                "",
            ]

        if lang in ('c', 'internal'):
            lines.append("_C language extension — see the Evergreen source code._")
        elif body.strip():
            lines.append(f"[source,{source_lang(lang)}]")
            lines.append("----")
            lines.append(body.rstrip())
            lines.append("----")
        lines.append("")

    return '\n'.join(lines) + '\n'


def render_write_safety(modifying_tables: list[dict]) -> str:
    lines = [
        "= Write Safety Reference",
        ":description: All tables with data-modifying triggers or trigger side effects.",
        "",
        "xref:index.adoc[↑ Home]",
        "",
        "This page consolidates all write-safety concerns across the database. "
        "Review this before bulk-inserting or updating data directly in SQL.",
        "",
        "== Data-Modifying Triggers (BEFORE ROW)",
        "",
        "These tables have BEFORE ROW triggers that silently rewrite column values. "
        "The data you write is *not* what gets stored.",
        "",
        '[cols="3,3,4"]',
        "|===",
        "|Table |Trigger |What it changes",
        "",
    ]
    for entry in modifying_tables:
        if not entry['modifying_triggers']:
            continue
        for trig in entry['modifying_triggers']:
            lines.append(f"| {xref_table(entry['schema'], entry['table'])}")
            lines.append(f"| `{esc(trig['trigger_name'])}`")
            lines.append(f"| Runs `{trig['function_schema']}.{trig['function_name']}()` BEFORE INSERT OR UPDATE")
            lines.append("")
    lines.append("|===")
    lines.append("")

    lines += [
        "== Trigger Side Effects (AFTER triggers writing to other tables)",
        "",
        "Writing to these tables automatically causes writes to other tables via AFTER triggers. "
        "This can cause unexpected locks or cascading failures in transactions.",
        "",
        '[cols="3,3,3,2"]',
        "|===",
        "|Table |Trigger |Writes to |Operation",
        "",
    ]
    for entry in modifying_tables:
        for se in entry.get('side_effects', []):
            lines.append(f"| {xref_table(entry['schema'], entry['table'])}")
            lines.append(f"| `{esc(se['trigger_name'])}`")
            lines.append(f"| {xref_table(se['target_schema'], se['target_table'])}")
            lines.append(f"| {se['op']}")
            lines.append("")
    lines.append("|===")
    lines.append("")

    return '\n'.join(lines) + '\n'


def render_nav(schemas: list[dict]) -> str:
    schema_by_name = {s['schema_name']: s for s in schemas}
    all_schema_names = {s['schema_name'] for s in schemas}

    lines = ["* xref:index.adoc[Home]", "* xref:write-safety.adoc[Write Safety]", ""]

    accounted = set()
    for group_name, group_schemas in NAV_GROUPS:
        group_schemas_present = [s for s in group_schemas if s in all_schema_names]
        if not group_schemas_present:
            continue
        lines.append(f".{group_name}")
        for sname in group_schemas_present:
            accounted.add(sname)
            lines.append(f"* xref:schemas/{sname}.adoc[{sname}]")
        lines.append("")

    remaining = [s for s in schemas if s['schema_name'] not in accounted]
    if remaining:
        lines.append(".Other")
        for s in remaining:
            lines.append(f"* xref:schemas/{s['schema_name']}.adoc[{s['schema_name']}]")
        lines.append("")

    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# File writer
# ---------------------------------------------------------------------------

class DocWriter:
    def __init__(self, output_dir: Path, nav_file: Path, dry_run: bool = False):
        self.output_dir = output_dir
        self.nav_file = nav_file
        self.dry_run = dry_run
        self.written = 0

    def write(self, relative_path: str, content: str):
        full_path = self.output_dir / relative_path
        if self.dry_run:
            print(f"  [dry-run] {full_path.relative_to(BASE_DIR)} ({len(content)} chars)")
        else:
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content, encoding='utf-8')
        self.written += 1

    def write_nav(self, content: str):
        if self.dry_run:
            print(f"  [dry-run] {self.nav_file.relative_to(BASE_DIR)} ({len(content)} chars)")
        else:
            self.nav_file.parent.mkdir(parents=True, exist_ok=True)
            self.nav_file.write_text(content, encoding='utf-8')
        self.written += 1


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def get_connection(dsn: str | None) -> psycopg2.extensions.connection:
    if dsn:
        conn = psycopg2.connect(dsn)
    elif os.environ.get('DATABASE_URI') or os.environ.get('DATABASE_URL'):
        conn = psycopg2.connect(os.environ.get('DATABASE_URI') or os.environ.get('DATABASE_URL'))
    else:
        conn = psycopg2.connect(
            host=os.environ.get('PGHOST', 'localhost'),
            port=os.environ.get('PGPORT', '5432'),
            dbname=os.environ.get('PGDATABASE', 'evergreen'),
            user=os.environ.get('PGUSER', 'evergreen'),
            password=os.environ.get('PGPASSWORD', ''),
        )
    conn.set_session(readonly=True, autocommit=False)
    return conn


def generate_all(conn, output_dir: Path, nav_file: Path,
                 schema_filter: str | None = None, dry_run: bool = False):
    db = DatabaseIntrospector(conn)
    writer = DocWriter(output_dir, nav_file, dry_run)
    gen_date = date.today().isoformat()

    print("Fetching schemas and global FK counts...")
    schemas = db.get_schemas()
    global_incoming_fk_counts = db.get_global_incoming_fk_counts()
    print(f"  Found {len(schemas)} schemas")

    # Determine which schemas have functions (for trigger function xrefs)
    schemas_with_functions: set[str] = {
        s['schema_name'] for s in schemas if s['function_count'] > 0
    }

    # Nav and home
    writer.write_nav(render_nav(schemas))

    # Collect write-safety data across all schemas
    all_write_safety: list[dict] = []

    for schema_info in schemas:
        sname = schema_info['schema_name']
        if schema_filter and sname != schema_filter:
            continue

        print(f"Processing schema: {sname} "
              f"({schema_info['table_count']} tables, "
              f"{schema_info['view_count']} views, "
              f"{schema_info['function_count']} functions)")

        tables = db.get_tables(sname)
        views = db.get_views(sname)
        enums = db.get_enums(sname)

        # Pre-compute trigger/fk counts for schema index
        trigger_counts: dict[int, int] = {}
        fk_counts: dict[int, int] = {}
        for t in tables:
            relid = t['relid']
            trigs = db.get_triggers(relid)
            trigger_counts[relid] = len(trigs)
            fks = db.get_foreign_keys(relid)
            fk_counts[relid] = len(fks)

        # Schema index page
        writer.write(
            f'schemas/{sname}.adoc',
            render_schema_index(schema_info, tables, views, enums,
                                trigger_counts, fk_counts)
        )

        # Table pages
        for t in tables:
            relid = t['relid']
            tname = t['table_name']
            columns = db.get_columns(relid)
            pk = db.get_primary_key(relid)
            fks = db.get_foreign_keys(relid)
            incoming = db.get_incoming_fks(relid)
            indexes = db.get_indexes(relid)
            triggers = db.get_triggers(relid)
            checks = db.get_check_constraints(relid)
            uniques = db.get_unique_constraints(relid)
            incoming_count = global_incoming_fk_counts.get((sname, tname), 0)

            # Collect write-safety data
            modifying = [tr for tr in triggers if is_data_modifying_trigger(tr)]
            side_fx = detect_side_effects(triggers, sname, tname)
            if modifying or side_fx:
                all_write_safety.append({
                    'schema': sname,
                    'table': tname,
                    'modifying_triggers': modifying,
                    'side_effects': side_fx,
                })

            writer.write(
                f'tables/{sname}/{sname}_{tname}.adoc',
                render_table_page(sname, tname, t, columns, pk, fks, incoming,
                                  indexes, triggers, checks, uniques,
                                  incoming_count, schemas_with_functions)
            )

        # View pages
        for v in views:
            vname = v['view_name']
            vcols = db.get_view_columns(v['relid'])
            writer.write(
                f'views/{sname}/{sname}_{vname}.adoc',
                render_view_page(sname, vname, v, vcols)
            )

        # Types page (enums)
        if enums:
            writer.write(
                f'types/{sname}.adoc',
                render_types_page(sname, enums)
            )

        # Functions page
        if schema_info['function_count'] > 0:
            functions = db.get_functions(sname)
            if functions:
                writer.write(
                    f'functions/{sname}.adoc',
                    render_functions_page(sname, functions)
                )

    # Write home page (after loop so we have write-safety data count)
    writer.write('index.adoc', render_home(schemas, gen_date))

    # Write-safety reference page
    writer.write('write-safety.adoc', render_write_safety(all_write_safety))

    print(f"\nDone. {'Would write' if dry_run else 'Wrote'} {writer.written} files.")


# ---------------------------------------------------------------------------
# DBML generation
# ---------------------------------------------------------------------------

def _pg_type_to_dbml(pg_type: str) -> str:
    """Map PostgreSQL type strings to DBML-compatible type names."""
    if not pg_type:
        return 'varchar'
    t = pg_type.strip()
    # Ordered longest-first so prefix matches don't short-circuit
    _MAP = [
        ('timestamp with time zone',    'timestamptz'),
        ('timestamp without time zone', 'timestamp'),
        ('time with time zone',         'timetz'),
        ('time without time zone',      'time'),
        ('character varying',           'varchar'),
        ('double precision',            'float8'),
        ('character(',                  'char('),  # char(n)
        ('bit varying',                 'varbit'),
        ('interval',                    'varchar'),  # interval has no DBML equivalent
    ]
    for pg, dbml in _MAP:
        if t.startswith(pg):
            return dbml + t[len(pg):]
    # Quote any remaining multi-word type so DBML parser doesn't misread words as settings
    if ' ' in t:
        return f'"{t}"'
    return t


def _dbml_default(default_val: str | None) -> str | None:
    """Return a DBML default: expression string, or None to omit."""
    if not default_val:
        return None
    # Sequence defaults are expressed as `increment` on the column; skip here
    if 'nextval(' in default_val:
        return None
    # Strip ::type casts — DBML doesn't understand them.
    # PostgreSQL casts can be multi-word: '09:00:00'::time without time zone
    # Split on the first '::' and keep only what's before it.
    val = default_val.split('::')[0].strip().strip("'\"")
    # Re-quote string values; pass numeric/boolean/function as raw expressions
    if default_val.startswith("'"):
        return f"'{val}'"
    return val


def _dbml_note(text: str | None, maxlen: int = 200) -> str | None:
    """Return a safe DBML note string (single-quoted, escaped)."""
    if not text:
        return None
    cleaned = text.replace("'", "\\'").replace('\n', ' ').replace('\r', '').strip()
    return cleaned[:maxlen]


def generate_dbml(conn, output_path: Path, dry_run: bool = False) -> None:
    """
    Query the database and write a DBML file suitable for dbdocs.io,
    dbdiagram.io, or any other DBML-compatible tool.

    Schema-qualified table names ("schema"."table") are used throughout so
    the full multi-schema structure is preserved in tools that support it.
    TableGroup blocks organise tables by schema for tools that render them.
    All foreign-key Ref statements are emitted at the end of the file.
    """
    db = DatabaseIntrospector(conn)
    gen_date = date.today().isoformat()

    print("Generating DBML...")
    schemas = db.get_schemas()

    lines: list[str] = [
        f'// Evergreen ILS — complete database schema',
        f'// Generated {gen_date}',
        f'// {sum(s["table_count"] for s in schemas)} tables across'
        f' {len(schemas)} schemas',
        f'//',
        f'// Upload to https://dbdocs.io  or  https://dbdiagram.io',
        '',
    ]

    all_refs: list[str] = []

    for schema_info in schemas:
        sname = schema_info['schema_name']
        tables = db.get_tables(sname)
        enums  = db.get_enums(sname)

        if not tables and not enums:
            continue

        lines += [
            f'// --------------------------------------------------------',
            f'// Schema: {sname}',
            f'// {SCHEMA_DESCRIPTIONS.get(sname, "")}',
            f'// --------------------------------------------------------',
            '',
        ]

        # TableGroup — organises tables visually by schema
        if tables:
            lines.append(f'TableGroup "{sname}" {{')
            for t in tables:
                lines.append(f'  "{sname}"."{t["table_name"]}"')
            lines.append('}')
            lines.append('')

        # Enums
        for enum in enums:
            note = _dbml_note(enum.get('type_comment'))
            note_str = f' [note: \'{note}\']' if note else ''
            lines.append(f'Enum "{sname}"."{enum["type_name"]}"{note_str} {{')
            for val in (enum['values'] or []):
                lines.append(f'  "{val}"')
            lines.append('}')
            lines.append('')

        # Tables
        for t in tables:
            tname  = t['table_name']
            relid  = t['relid']

            columns       = db.get_columns(relid)
            pk_cols       = set(db.get_primary_key(relid))
            fks           = db.get_foreign_keys(relid)
            unique_constr = db.get_unique_constraints(relid)

            # Single-column unique constraints (multi-col are just indexes)
            unique_single = {
                uc['columns'][0]
                for uc in unique_constr
                if len(uc['columns'] or []) == 1
            }

            # Table note from comment
            tbl_note = _dbml_note(t.get('table_comment'))
            tbl_note_str = f' [note: \'{tbl_note}\']' if tbl_note else ''

            lines.append(f'Table "{sname}"."{tname}"{tbl_note_str} {{')

            for col in columns:
                cname    = col['column_name']
                ctype    = _pg_type_to_dbml(col['data_type'])
                attrs: list[str] = []

                if cname in pk_cols:
                    attrs.append('pk')

                if 'nextval(' in (col['column_default'] or ''):
                    attrs.append('increment')
                elif not col['is_nullable'] and cname not in pk_cols:
                    attrs.append('not null')

                if cname in unique_single and cname not in pk_cols:
                    attrs.append('unique')

                dflt = _dbml_default(col['column_default'])
                if dflt is not None:
                    attrs.append(f'default: `{dflt}`')

                col_note = _dbml_note(col.get('column_comment'))
                if col_note:
                    attrs.append(f"note: '{col_note}'")

                attr_str = f' [{", ".join(attrs)}]' if attrs else ''
                lines.append(f'  "{cname}" {ctype}{attr_str}')

            lines.append('}')
            lines.append('')

            # Collect Ref statements
            for fk in fks:
                lcols  = fk['local_columns'] or []
                fcols  = fk['foreign_columns'] or []
                fs     = fk['foreign_schema']
                ft     = fk['foreign_table']
                if not lcols or not fcols:
                    continue
                if len(lcols) == 1:
                    ref = (f'Ref: "{sname}"."{tname}"."{lcols[0]}"'
                           f' > "{fs}"."{ft}"."{fcols[0]}"')
                else:
                    lpart = '(' + ', '.join(f'"{c}"' for c in lcols) + ')'
                    fpart = '(' + ', '.join(f'"{c}"' for c in fcols) + ')'
                    ref = f'Ref: "{sname}"."{tname}".{lpart} > "{fs}"."{ft}".{fpart}'
                all_refs.append(ref)

    # Emit all refs at the end (keeps table blocks clean)
    if all_refs:
        lines += [
            '// --------------------------------------------------------',
            '// Foreign Key References',
            '// --------------------------------------------------------',
            '',
        ]
        lines.extend(all_refs)
        lines.append('')

    content = '\n'.join(lines)

    if dry_run:
        print(f"  [dry-run] {output_path} ({len(content):,} chars,"
              f" {len(all_refs)} refs)")
        print(f"  [dry-run] {ATTACHMENTS_DIR / output_path.name} (copy for Antora download)")
    else:
        output_path.write_text(content, encoding='utf-8')
        print(f"  Wrote {output_path} ({len(content):,} chars, {len(all_refs)} refs)")
        # Also publish as an Antora attachment so the homepage download link works
        ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
        attachment = ATTACHMENTS_DIR / output_path.name
        attachment.write_text(content, encoding='utf-8')
        print(f"  Copied to {attachment.relative_to(BASE_DIR)} (Antora attachment)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description='Generate Antora docs and DBML for Evergreen ILS')
    p.add_argument('--dsn', help='PostgreSQL DSN (default: uses DATABASE_URI or PG* env vars)')
    p.add_argument('--schema', help='Only process a single schema (docs only, not DBML)')
    p.add_argument('--output-dir', type=Path, default=OUTPUT_DIR)
    p.add_argument('--nav-file', type=Path, default=NAV_FILE)
    p.add_argument('--dbml-output', type=Path, default=DBML_OUTPUT,
                   help='Path for generated DBML file (default: ./evergreen.dbml)')
    p.add_argument('--skip-dbml', action='store_true',
                   help='Skip DBML generation')
    p.add_argument('--dbml-only', action='store_true',
                   help='Generate only the DBML file, skip Antora docs')
    p.add_argument('--dry-run', action='store_true', help='Print what would be written')
    return p.parse_args()


def main():
    args = parse_args()
    try:
        conn = get_connection(args.dsn)
    except psycopg2.OperationalError as e:
        print(f"ERROR: Could not connect: {e}", file=sys.stderr)
        print("Set DATABASE_URI, or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD, or use --dsn",
              file=sys.stderr)
        sys.exit(1)

    try:
        if args.dbml_only:
            generate_dbml(conn, args.dbml_output, dry_run=args.dry_run)
        else:
            generate_all(conn, args.output_dir, args.nav_file,
                         schema_filter=args.schema, dry_run=args.dry_run)
            if not args.skip_dbml and not args.schema:
                generate_dbml(conn, args.dbml_output, dry_run=args.dry_run)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
