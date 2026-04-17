"""
Generate Antora changelog pages for the Evergreen ILS database schema.

Parses the migration files from the Evergreen ILS source tree to produce
per-version AsciiDoc pages listing which database migrations were applied,
including the actual SQL changes from each migration file.

Usage:
    uv run python scripts/generate_changelog.py [--evergreen-src PATH] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "modules" / "ROOT" / "pages"

# Patterns for parsing version-upgrade rollup files
VER_UPGRADE_RE = re.compile(r'^([\d.]+)-([\d.]+)-upgrade-db$')
DEPS_CHECK_RE = re.compile(r"upgrade_deps_block_check\('(\d+)'")

# Lines to strip from individual migration files (boilerplate only)
_BOILERPLATE_RES = [re.compile(p, re.IGNORECASE) for p in [
    r'^\s*BEGIN\s*;?\s*$',
    r'^\s*COMMIT\s*;?\s*$',
    r'^\s*\\set\s+eg_version\b',
    r'^\s*\\qecho\b',
    r'^\s*INSERT\s+INTO\s+config\.upgrade_log\b',
    r'^\s*SELECT\s+evergreen\.upgrade_deps_block_check\b',
    r'^\s*SELECT\s+auditor\.update_auditors\b',
]]


def ver_key(v: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in v.split('.'))
    except ValueError:
        return (0,)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def clean_migration_sql(content: str) -> str:
    """Strip transaction boilerplate from a numbered migration file body."""
    cleaned = []
    for line in content.splitlines():
        if any(r.match(line) for r in _BOILERPLATE_RES):
            continue
        cleaned.append(line)
    return '\n'.join(cleaned).strip()


def parse_migration_files(pg_dir: Path) -> dict[str, dict]:
    """
    Scan the upgrade/ directory and return {migration_id: {type, description, sql}}
    for every numbered migration file.

    Filename format: NNNN.TYPE.description-words.sql
    Some files lack a TYPE segment or a .sql extension — both are handled.
    """
    upgrade_dir = pg_dir / 'upgrade'
    result: dict[str, dict] = {}
    for f in upgrade_dir.iterdir():
        if not f.is_file():
            continue
        # Strip .sql suffix if present, then parse the name segments
        name = f.name[:-4] if f.name.endswith('.sql') else f.name
        parts = name.split('.', 2)
        if len(parts) < 2 or not parts[0].isdigit():
            continue
        num = parts[0].lstrip('0') or '0'
        raw_type = parts[1]
        if len(parts) > 2:
            mtype = raw_type
            desc = parts[2].replace('-', ' ').replace('_', ' ')
        elif '-' in raw_type or '_' in raw_type:
            # No type separator — the whole second segment is a description
            mtype = '—'
            desc = raw_type.replace('-', ' ').replace('_', ' ')
        else:
            mtype = raw_type
            desc = ''

        try:
            raw = f.read_text(encoding='utf-8', errors='replace')
        except OSError:
            raw = ''

        result[num] = {
            'type': mtype,
            'description': desc,
            'file': f.name,
            'sql': clean_migration_sql(raw),
        }
    return result


def parse_version_upgrades(pg_dir: Path) -> list[dict]:
    """
    Scan the version-upgrade/ directory.
    Returns list of dicts sorted by target version:
        {from_ver, to_ver, migration_ids: [str]}
    """
    version_dir = pg_dir / 'version-upgrade'
    entries = []
    for f in version_dir.glob('*-upgrade-db.sql'):
        m = VER_UPGRADE_RE.match(f.stem)
        if not m:
            continue
        from_ver, to_ver = m.group(1), m.group(2)
        content = f.read_text(encoding='utf-8', errors='replace')
        mig_ids = [s.lstrip('0') or '0' for s in DEPS_CHECK_RE.findall(content)]
        entries.append({
            'from': from_ver,
            'to': to_ver,
            'migration_ids': mig_ids,
        })
    entries.sort(key=lambda e: ver_key(e['to']))
    return entries


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def render_version_page(entry: dict, migrations: dict[str, dict]) -> str:
    to_ver = entry['to']
    from_ver = entry['from']
    mig_ids = entry['migration_ids']

    lines = [
        f"= Schema Changes: {to_ver}",
        f":description: Database schema changes applied in Evergreen {to_ver}",
        "",
        "xref:../changelog.adoc[↑ Changelog]",
        "",
        f"*Upgrade:* {from_ver} → {to_ver}",
        "",
    ]

    if not mig_ids:
        lines += [
            "[NOTE]",
            "====",
            "No schema migrations were applied in this release.",
            "====",
            "",
        ]
        return '\n'.join(lines) + '\n'

    lines += [
        f"This release applied {len(mig_ids)} migration(s) to the database schema.",
        "",
        '[cols="1,1,4"]',
        "|===",
        "|ID |Type |Description",
        "",
    ]

    for mid in mig_ids:
        info = migrations.get(mid)
        mtype = info['type'] if info else '—'
        desc = info['description'] if info else '_(file not found)_'
        lines.append(f"| <<mig-{mid},{mid}>>")
        lines.append(f"| {mtype}")
        lines.append(f"| {desc}")
        lines.append("")

    lines.append("|===")
    lines.append("")

    # Detailed SQL sections
    lines += ["== Migration Details", ""]

    for mid in mig_ids:
        info = migrations.get(mid)
        if info:
            mtype = info['type']
            desc = info['description']
            sql = info['sql']
        else:
            mtype = '—'
            desc = '_(file not found in upgrade/ directory)_'
            sql = ''

        heading = f"{mid}"
        if desc:
            heading += f" — {desc}"

        lines += [
            f"[[mig-{mid}]]",
            f"=== {heading}",
            "",
        ]

        if mtype != '—':
            lines += [f"*Type:* `{mtype}`", ""]

        if sql:
            lines += [
                "[%collapsible]",
                ".View SQL",
                "====",
                "[source,sql]",
                "----",
                sql,
                "----",
                "====",
                "",
            ]
        else:
            lines += ["_No SQL content available._", ""]

    return '\n'.join(lines) + '\n'


def render_changelog_index(entries: list[dict]) -> str:
    lines = [
        "= Database Schema Changelog",
        ":description: Version history of Evergreen ILS database schema changes.",
        "",
        "xref:index.adoc[↑ Home]",
        "",
        "_Auto-generated from migration files in the Evergreen ILS source tree._",
        "",
        '[cols="2,2,1"]',
        "|===",
        "|Version |Upgrade From |Migrations",
        "",
    ]

    for entry in reversed(entries):  # newest first
        to_ver = entry['to']
        from_ver = entry['from']
        count = len(entry['migration_ids'])
        lines.append(f"| xref:changelog/{to_ver}.adoc[{to_ver}]")
        lines.append(f"| {from_ver}")
        lines.append(f"| {count if count else '—'}")
        lines.append("")

    lines.append("|===")
    lines.append("")
    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def write_file(path: Path, content: str, dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] {path.relative_to(BASE_DIR)} ({len(content)} chars)")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def ensure_nav_entry(dry_run: bool = False) -> None:
    """Add the Changelog link to nav.adoc if not already present."""
    nav_file = BASE_DIR / "modules" / "ROOT" / "nav.adoc"
    entry = "* xref:changelog.adoc[Changelog]"
    if not nav_file.exists():
        return
    content = nav_file.read_text(encoding='utf-8')
    if entry in content:
        return
    updated = content.replace(
        "* xref:write-safety.adoc[Write Safety]",
        f"* xref:write-safety.adoc[Write Safety]\n{entry}",
        1,
    )
    if dry_run:
        print(f"  [dry-run] {nav_file.relative_to(BASE_DIR)} (add Changelog entry)")
    else:
        nav_file.write_text(updated, encoding='utf-8')
        print("  Updated nav.adoc with Changelog entry")


def generate_changelog(src: Path, output_dir: Path, dry_run: bool = False) -> None:
    pg_dir = src / 'Open-ILS' / 'src' / 'sql' / 'Pg'

    if not pg_dir.is_dir():
        print(f"ERROR: Expected Evergreen SQL directory not found: {pg_dir}", file=sys.stderr)
        sys.exit(1)

    print("Parsing migration files...")
    migrations = parse_migration_files(pg_dir)
    print(f"  Found {len(migrations)} numbered migration files")

    print("Parsing version-upgrade files...")
    entries = parse_version_upgrades(pg_dir)
    print(f"  Found {len(entries)} version-upgrade files")

    print("Writing pages...")
    for entry in entries:
        content = render_version_page(entry, migrations)
        path = output_dir / 'changelog' / f"{entry['to']}.adoc"
        write_file(path, content, dry_run)

    index_content = render_changelog_index(entries)
    write_file(output_dir / 'changelog.adoc', index_content, dry_run)

    ensure_nav_entry(dry_run=dry_run)

    action = 'Would write' if dry_run else 'Wrote'
    print(f"\nDone. {action} {len(entries)} version pages + 1 index.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description='Generate Antora changelog pages from Evergreen migration files'
    )
    p.add_argument(
        '--evergreen-src',
        help='Path to the Evergreen ILS source tree (default: EVERGREEN_SRC env var)',
    )
    p.add_argument(
        '--output-dir',
        type=Path,
        default=OUTPUT_DIR,
        help=f'Output directory for AsciiDoc pages (default: {OUTPUT_DIR})',
    )
    p.add_argument(
        '--dry-run',
        action='store_true',
        help='Print what would be written without writing files',
    )
    return p.parse_args()


def main():
    args = parse_args()
    src_str = args.evergreen_src or os.environ.get('EVERGREEN_SRC')
    if not src_str:
        print(
            "ERROR: Provide --evergreen-src PATH or set EVERGREEN_SRC in .env",
            file=sys.stderr,
        )
        sys.exit(1)
    src = Path(src_str)
    if not src.is_dir():
        print(f"ERROR: Not a directory: {src}", file=sys.stderr)
        sys.exit(1)

    generate_changelog(src, args.output_dir, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
