import asyncio

from core.flows.predemux import production_demux_flow  # type: ignore


if __name__ == "__main__":
    # Example usage
    run_path = "/home/anastasios/Documents/git/dmx/mock_miarka/incoming/illumina/novaseqxplus/20250601_LH001_0001_ABCDEFGHIJ"
    instrument = "novaseqxplus"
    result = asyncio.run(production_demux_flow(run_path, instrument))
    # Note: In a real scenario, this would be run by Prefect's orchestration system.
