import pathlib

import click
from rich.table import Table

import pydupe.dupetable as Dupetable
from pydupe.console import console
from pydupe.db import PydupeDB
from pydupe.hasher import Hasher
from pydupe.mover import Mover


@click.group()
@click.option('-db', '--dbname', required=False, default=str(pathlib.Path.home())+'/.pydupe.sqlite', show_default=True, help='sqlite Database')
@click.option('-tr', '--trash', required=False, default='/opt/chris/.pydupeTrash', show_default=True, help='path to Trash')
@click.option('-of', '--outfile', required=False, default=str(pathlib.Path.home())+'/dupestree.html', show_default=True, help='html output for inspection in browser')
@click.version_option()
@click.pass_context
def cli(ctx, dbname, trash, outfile):
    ctx.obj = {
        'dbname': dbname,
        'trash': trash,
        'outfile': outfile
    }


@cli.command()
@click.argument('depth', default=10)
@click.pass_context
def lst(ctx, depth):
    """
    list directories with the most dupes until DEPTH.
    """
    table = Table()
    table.add_column("Directory", justify="right", style="cyan", no_wrap=True)
    table.add_column("# of dupes", justify="left", style="green")
    dbname = ctx.obj['dbname']
    d = Dupetable.Dupetable(dbname)
    dc = d.get_dir_counter()
    for f, c in dc.most_common(depth):
        table.add_row(f, str(c))
    console.print(table)


@cli.command()
@click.option('--match_deletions/--match_keeps', default=True, show_default=True, help='chooose [PATTERN] matches to select dupes to delete or dupes to keep.')
@click.option('--autoselect/--no-autoselect', default=False, show_default=True, help='autoselect dupes to keep if all are mached')
@click.option('--dupes_global/--dupes_local', default=True, show_default=True, help='consider dupes outside chosen directory')
@click.option('--do_move/--no-do_move', default=False, show_default=True, help='if True, dupes are moved to trash')
@click.argument('deldir', required=True)
@click.argument('pattern', required=False, default=".")
@click.pass_context
def dd(ctx, match_deletions, autoselect, dupes_global, do_move, deldir, pattern):
    """\b
    1) Dupes are selected by regex search PATTERN within DELDIR. PATTERN is optional and defaults to '.' (any character).
       Note the regex search (not match).
    2) Hits within DELDIR are marked as dupes to delete (match_deletions) or dupes to keep (match_keeps).
    3) If dupes_global is True (default), all dupes outside DELDIR are taken into account:
        - if match_deletions, they are added to the list of dupes to keep,
        - if match_keeps, they are marked for deletion.
    4) if autoselect ist True and all dupes of a files are marked for deletion, one of them is deselected;
       if autoselect is False (the default), no deletion is performed.

    if do_move is False, a dry run is done and a pretty printed table is printed and saved as html for further inspection.
    if do_move is True, dupes marked for deletion will be moved to TRASH and deleted from the database.
    """
    dbname = ctx.obj['dbname']
    trash = ctx.obj['trash']
    outfile = ctx.obj['outfile']
    d = Dupetable.Dupetable(dbname)
    deltable, keeptable = d.dd3(
        deldir, pattern, match_deletions=match_deletions, dupes_global=dupes_global, autoselect=autoselect)

    # check keeptable
    mver = Mover(dbname)
    if do_move:
        mver.do_move(deltable=deltable, trash=trash)
    else:
        mver.print_dupestree(
            deltable=deltable, keeptable=keeptable, outfile=outfile)


@cli.command()
@click.argument('path', required=True, type=click.Path(exists=True))
@click.pass_context
def hash(ctx, path):
    """
    recursive hash files in PATH and store hash in database.
    """
    dbname = ctx.obj['dbname']
    hsr = Hasher(dbname)
    hsr.hashdir(path)


@cli.command()
@click.argument('path', required=True, type=click.Path(exists=True))
@click.pass_context
def reducedb(ctx, path):
    """
    reduce database to files within PATH.
    """
    dbname = ctx.obj['dbname']
    with PydupeDB(dbname) as db:
        db.reduce_to_dir(path)
        db.commit()


@cli.command()
@click.pass_context
def sanitizedb(ctx):
    """
    sanitize hashes in database.
    """
    dbname = ctx.obj['dbname']
    with PydupeDB(dbname) as db:
        db.sanitize_hash_regex()
        db.clear_tmp()
        db.commit()


@cli.command()
def help():
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
