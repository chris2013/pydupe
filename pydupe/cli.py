import logging
import typing
from datetime import datetime
from fcntl import F_SEAL_SEAL
from pathlib import Path as p

import rich_click as click

import pydupe.dupetable as dupetable
from pydupe.cmd import cmd_hash, cmd_purge
from pydupe.console import console
from pydupe.db import PydupeDB


@click.group()
@click.option('-db', '--dbname', required=False, default=p.home() / '.pydupe.sqlite', show_default=True, help='sqlite Database', type=click.Path(path_type=p))
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, dbname: p) -> None:
    ctx.obj = {
        'dbname': dbname,
    }


@cli.command()
@click.argument('depth', default=10)
@click.pass_context
def lst(ctx: click.Context, depth: int) -> None:
    """
    list directories with the most dupes until DEPTH.
    """
    dbname = ctx.obj['dbname']
    Dp = dupetable.Dupes(dbname)
    Dp.print_most_common(depth)


@cli.command()
@click.option('--match_deletions/--match_keeps', default=True, show_default=True, help='chooose [PATTERN] matches to select dupes to delete or dupes to keep.')
@click.option('--autoselect', is_flag=True, default=False, help='autoselect dupes if all are matched')
@click.option('--dupes_global/--dupes_local', default=True, show_default=True, help='consider dupes outside chosen directory')
@click.option('--do_move', is_flag=True, default=False, show_default=True, help='dupes are movedi if this flag is set')
@click.option('--delete/--trash', default=False, show_default=True, help='delete dupes or use trash')
@click.option('-tr', '--trash', required=False, default=p.home() / '.pydupeTrash', show_default=True, help='path to Trash. If set to "DELETE", no trash is used.', type=click.Path(path_type=p))
@click.option('-of', '--outfile', required=False, default=p.home() / 'dupestree.html', show_default=True, help='html output for inspection in browser', type=click.Path(path_type=p))
@click.argument('deldir', required=True, type=click.Path(exists=True, path_type=p))
@click.argument('pattern', required=False, default=".")
@click.pass_context
def dd(ctx: click.Context, match_deletions: bool, autoselect: bool, dupes_global: bool, do_move: bool, delete: bool, trash: p, outfile: p, deldir: p, pattern: str) -> None:
    """
    Dedupe Directory. Type dd --help for details.
    \b
    1) Dupes are selected by regex search PATTERN within DELDIR. PATTERN is optional and defaults to '.' (any character).
       Note the regex search (not match).
    2) Hits within DELDIR are marked as dupes to delete (if match_deletions, default) or dupes to keep (if match_keeps).
    3) If dupes_global is True (default), all dupes outside DELDIR are taken into account:
        - if match_deletions, they are added to the list of dupes to keep,
        - if match_keeps, they are marked for deletion.
    4) if autoselect ist True and all dupes of a files are marked for deletion, one of them is deselected;
       if autoselect is False (the default), no deletion is performed if for a hash dupes are not contained in keeptable.
    if do_move is False, a dry run is done and a pretty printed table is shown and saved as html for further inspection.
    """
    option: typing.Dict[str, typing.Any] = {}
    option['dbname'] = ctx.obj['dbname']
    option['deldir'] = deldir.resolve()
    option['pattern'] = pattern
    option['match_deletions'] = match_deletions
    option['dupes_global'] = dupes_global
    option['autoselect'] = autoselect
    option['dedupe'] = True
    Dt: dupetable.Dupetable = dupetable.Dupetable(**option)

    dupestree, dels, keeps = Dt.get_tree()

    console.record = True
    console.print(f"[red]deletions: {dels} [green]keeps: {keeps}")
    console.print(dupestree)
    if outfile:
        console.save_html(str(outfile))
    console.record = False

    if do_move:
        Dt.delete(trash, delete)


@cli.command()
@click.argument('path', required=True, type=click.Path(exists=True, path_type=p))
@click.pass_context
def hash(ctx: click.Context, path: p) -> None:
    """
    recursive hash files in PATH and store hash in database.
    """
    cmd_hash(dbname=ctx.obj['dbname'], path=path.resolve())


@cli.command()
@click.pass_context
def purge(ctx: click.Context) -> None:
    """
    purge database: delete lookup and all files in permanent that are not available anymore
    """
    dbname = ctx.obj['dbname']
    cmd_purge(dbname)

@cli.command()
@click.pass_context
def clean(ctx: click.Context) -> None:
    """
    clean database: delete lookup
    """
    dbname = ctx.obj['dbname']
    cmd_purge(dbname)
@cli.command()
def help() -> None:
    """
    Display some useful expressions for exiftool.
    """
    click.echo("\nembedded exiftool help:")
    click.echo(
        "show dateTimeOriginal for all files:\texiftool -p '$filename $dateTimeOriginal' .")
    click.echo(
        "set dateTimeOrginial for all files: \texiftool -dateTimeOriginal='YYYY:mm:dd HH:MM:SS' .")
    click.echo(
        "rename files: \t\t\t\texiftool -filename=newname . {%f: filename %e: extension %c copynumber}")
    click.echo("move file in dir to newdir/YEAR/MONTH: \texiftool -progress -recurse '-Directory<DateTimeOriginal' -d newdir/%Y/%m dir")
    click.echo("\n\nembedded unix tool help:")
    click.echo("find/delete empty dirs: \t\t\tfind . -type d -empty <-delete>")
