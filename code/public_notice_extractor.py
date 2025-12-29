#!/usr/bin/env python3

from prefect import flow, get_run_logger
from utils.configs_utils import load_env_vars, read_configs
from utils.import_utils import import_template_module, validate_template_module
from utils.sysargs_utils import get_sysargs

@flow(cache_result_in_memory = False)
def main():
    """
    Public notice extractor entry point.

    NOTE: All Prefect flows and tasks should have result caching and persistence disabled. 
    Browser automation depends on real-time page state, so replaying a cached result is 
    never useful. Browser objects cannot be serialized in a reliable way and saving page
    content would create unnecessary storage and network overhead without adding any real value. 
    Turning persistence off makes the workflow lighter, faster, and consistent with the real-time 
    nature of the extraction process.
    """
    logger = get_run_logger()

    logger.info("🌱 Started runtime environment setup")
    load_env_vars()
    general_configs = read_configs("general")
    sysargs = get_sysargs()
    logger.info("🌺 Finished runtime environment setup")

    for state in sysargs["states"]:
        logger.info(f"🟢 Started extraction processing loop for {state}")
        state_configs = read_configs(state, state)

        logger.info(f"🚛 Started dynamic template import for {state}")
        template_module = import_template_module(state, state_configs["template"]["group_id"])
        validate_template_module(template_module, general_configs["constants"]["REQUIRED_TEMPLATE_MODULE_FUNCTIONS"])
        logger.info(f"📦 Finished dynamic template import for {state}")

        extraction_context = {
            "counties": sysargs["counties"],
            "state": state,
            "general_configs": general_configs,
            "state_configs": state_configs,
            "keywords": sysargs["keywords"],
            "search_phrase": sysargs["search_phrase"],
            "target_env": sysargs["target_env"]
        }

        template_module.run_extraction_template(extraction_context)

        logger.info(f"🔴 Finished extraction processing loop for {state}")
    logger.info(f"🏁 Finished all extractions")

if __name__ == "__main__":
    main()