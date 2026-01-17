import great_expectations as gx
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def run_ge_validation(
    records: list[dict],
    expectation_fn,
    source_name: str
) -> None:
    if not records:
        raise ValueError(f"[GE] {source_name}: No records to validate")

    df = pd.DataFrame(records)

    context = gx.get_context()

    data_source = context.data_sources.add_pandas(name="runtime_pandas")

    data_asset = data_source.add_dataframe_asset(name=f"{source_name}_asset")

    batch_def = data_asset.add_batch_definition_whole_dataframe(f"{source_name}_batch_definition")

    batch = batch_def.get_batch(batch_parameters={"dataframe":df})

    suite_name = f"{source_name}_suite"

    context.suites.add(gx.ExpectationSuite(name=suite_name))

    validator = context.get_validator(
        batch=batch,
        expectation_suite_name=suite_name
    )

    expectation_fn(validator)

    result = validator.validate()

    if not result.success:
        failed = [
            r.expectation_config.expectation_type
            for r in result.results
            if not r.success
        ]

        raise ValueError(
            f"[GE] Validation failed for {source_name}. "
            f"Failed expectations: {failed}"
        )

    logger.info(
        "[GE] Validation passed for %s (%s rows)",
        source_name,
        len(df)
    )
