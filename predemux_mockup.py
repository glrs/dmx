import argparse
import logging

def prepare_flow_cell(flow_cell_id):
    """
    Prepare the flow cell for demultiplexing by locating the samplesheet,
    reading it, separating samples into groups, and generating sub-sample sheets.
    
    Args:
        flow_cell_id (str): The ID of the flow cell to prepare.
    
    Returns:
        None
    """
    logger.info(f"Preparing flow cell {flow_cell_id} for demultiplexing.")
    # Locate the samplesheet based on the given flow cell
    # Read the samplesheet
    # Separate the samples into groups based on lane and sample type
    # Generate sub-sample sheets for each group
    # Generate demux command for each group


def main():
    parser = argparse.ArgumentParser("Prepare sequencing run for demultiplexing")
    parser.add_argument(
        "-f",
        "--flow_cell",
        default=None,
        action="store",
        help="Flow cell ID to prepare for demultiplexing, e.g. '20250528_LH00217_0219_A22TT52LT4'",
    )
    
    kwargs = vars(parser.parse_args())
    prepare_flow_cell(kwargs["flow_cell"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.debug("Starting up predemux CLI")
    main()