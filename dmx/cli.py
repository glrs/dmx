import click
from dmx.dmx import prepare_demux

@click.command()
@click.option('-r', '--run', required=True, help='The run identifier, e.g., 20250528_LH00217_0219_A22TT52LT4.')
def cli(run):
    """
    Command line interface for the dmx module.
    
    This CLI allows users to specify a run identifier and processes it accordingly.
    
    Args:
        run (str): The run identifier, e.g., '20250528_LH00217_0219_A22TT52LT4'.
    """
    prepare_demux(run=run)