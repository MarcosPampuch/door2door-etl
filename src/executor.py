import uuid
import traceback
from helper.helper import check_inputs_consistency
from art import text2art
import click
from datetime import datetime, timezone
from ingestor import ingestor
from handler import handler
from helper.logger import logger

@click.command()
@click.option(
    "-s",
    "--step",
    required=True,
    default="all",
    type=click.Choice(
        ["all", "ingestor", "handler"],
        case_sensitive=False,
    ),
    help="The step mode to be executed.",
)
@click.option(
    "--workflow",
    "-w",
    required=False,
    default=None,
    help="the worflow_id to be executed. Should be used ONLY when variable 'step' is 'handler'.",
)

def executor(step, workflow) -> None:

    check_inputs_consistency(step, workflow)

    if not workflow:
        workflow_id = str(uuid.uuid4())
    else:
        workflow_id = workflow
    
    logger.info(f"Starting workflow {workflow_id} -- step(s): {'ingestor and handler'if step == 'all' else step}")


    if step in ('ingestor', 'all'):
        ingestor.main(workflow_id)
    
    
    if step in ('handler', 'all'):
        handler.main(workflow_id)
    


if __name__ == "__main__":
    executor()
