#!/usr/bin/env python3

from connectors.core import load_payload, stage_payload
from drivers.driver_factory import get_driver
from prefect import get_run_logger, task
from utils.configs_utils import read_configs, write_configs
from utils.datetime_utils import is_notice_within_lookback_period
from utils.import_utils import import_executor_module, validate_executor_module

@task(persist_result = False, cache_result_in_memory = False)
def run_extraction_template(extraction_context):
    """
    Runs the public notice extraction when the state template is `group_id_1`.

    Parameters:
        extraction_context (dict): A dictionary containing context-specific information 
                                   used for extraction. The dictionary includes the 
                                   following:
                                           counties (list[str]): A list of counties to process.
                                                    state (str): The abbreviated name of a US state.
                                        project_root_path (str): The absolute path to the root of the project directory.
                                         general_configs (dict): Dictionary containing general configuration values.
                                           state_configs (dict): Dictionary containing state-specific configuration values.
                                           keywords (list[str]): A list of keywords used to filter relevant public notices.
                                            search_phrase (str): A search phrase to narrow search results down by.
                                               target_env (str): Specifies which target environment should be used during runtime.
    """
    logger = get_run_logger()

    # Unpacking for readability
    counties = extraction_context["counties"]
    state = extraction_context["state"]
    general_configs = extraction_context["general_configs"]
    state_configs = extraction_context["state_configs"]
    keywords = extraction_context["keywords"]
    search_phrase = extraction_context["search_phrase"]
    target_env = extraction_context["target_env"]

    # Template variables
    template_group_id = state_configs["template"]["group_id"]
    driver_name = state_configs["browser"]["driver"]
    is_headless = state_configs["browser"]["is_headless"]

    # Executor variables
    page_timeout = state_configs["defaults"]["page_timeout"]
    navigation_timeout = state_configs["defaults"]["navigation_timeout"]
    metadata_timeout = state_configs["executor"]["metadata_timeout"]
    did_search_return_nothing_timeout = state_configs["executor"]["did_search_return_nothing_timeout"]
    public_notice_url = state_configs["executor"]["public_notice_url"]
    recaptcha_v2_timeout = state_configs["resolver"]["recaptcha_v2_timeout"]

    # Although this variable represents a number, it's stored as a string because JavaScript and browsers treat all form values 
    # as strings when submitting or interacting with the DOM. If you try to pass an integer (like 20 instead of "20"), 
    # the browser driver won't find a matching string value and the selection will fail.
    # Consequently, cast this variable to an integer if performing any arithmetic 
    # to avoid type issues, whereas applicable in downstream tasks.
    notices_per_page = state_configs["executor"]["notices_per_page"]

    logger.info(f"🚚 Started dynamic executor import for {state}")
    REQUIRED_EXECUTOR_MODULE_FUNCTIONS = general_configs["constants"]["REQUIRED_EXECUTOR_MODULE_FUNCTIONS"]
    executor_module = import_executor_module(state, template_group_id, driver_name)
    validate_executor_module(executor_module, REQUIRED_EXECUTOR_MODULE_FUNCTIONS)
    logger.info(f"📦 Finished dynamic executor import for {state}")

    num_all_counties = len(state_configs["defaults"]["counties"])
    counties_to_extract = ["all"] if len(counties) == num_all_counties else counties
    for county in counties_to_extract:
        county_name = "all counties" if county == "all" else county
        logger.info(f"🟢 Started extraction processing loop for {county_name}, {state}")
        
        current_county = "all" if county is None else county
        county_configs = read_configs(current_county, state, current_county)

        logger.info(f"🚙 Started dynamic driver session for {county_name}, {state}")
        with get_driver(driver_name, is_headless) as driver:

            logger.info(f"⛏ Started extraction for {county_name}, {state}")
            executor_module.bootstrap_extraction(driver, public_notice_url, page_timeout, navigation_timeout)

            for keyword_index, keyword in enumerate(keywords):
                logger.info(f"🔍 Started search for {keyword} in {county_name}, {state}")
                executor_module.set_search_parameters(driver, keyword, search_phrase, county, keyword_index)
                executor_module.submit_search_parameters(driver)

                # It's crucial to determine the number of results as the website's DOM structure
                # changes when there are more than 10. This impacts how metadata is extracted
                # in downstream processing.
                num_results = executor_module.evaluate_presentation_layer(driver, did_search_return_nothing_timeout, notices_per_page)

                if not num_results:
                    is_processing_notices = False
                    logger.info(f"❌ No results found for {keyword} in {county_name}, {state}")
                    break

                # Initialize current keyword runtime variables
                # This logic sets the iteration range for the notice processing loop
                # according to the upstream `num_results`.
                if num_results <= 10:
                    head_notice_node = state_configs["executor"]["head_notice_node"] - 1
                    post_tail_notice_index = 12
                else:
                    head_notice_node = state_configs["executor"]["head_notice_node"]
                    post_tail_notice_index = int(notices_per_page) + 3

                notice_number = head_notice_node - 2 if head_notice_node == 3 else head_notice_node - 1
                pagination_node = int(notices_per_page) + 2
                head_notice_publisher = None
                head_notice_publication_date = None
                head_notice_excerpt = None
                is_keyword_new_to_system = False
                days_to_look_back = None

                # This boolean indicates whether pagination occurred during the current keyword search.
                # This is important because we cache the metadata of "the first"/head notice to later
                # update the tail notice metadata in the county’s configuration file.
                # If pagination takes place, we do not want to overwrite this cached metadata.
                did_current_keyword_trigger_pagination = False

                # If the current keyword is entirely new to the system then there will be no existing tail notice metadata.
                # In this case, we’ll need to initialize it using runtime data, assuming any results are returned.
                # To avoid extracing notices that are too outdated to be relevant, the search/notice processing loop
                # will be restricted to those published within the last X days from the current date. Where X is 
                # defined by the `days_to_look_back` key from the `defaults` table in the general TOML configuration file.
                tail_notice_metadata = county_configs["tail_notice_metadata"][keyword]
                if tail_notice_metadata["publisher"] == "" or \
                    tail_notice_metadata["publication_date"] == "" or \
                    tail_notice_metadata["excerpt"] == "":
                    is_keyword_new_to_system = True
                    days_to_look_back = general_configs["defaults"]["days_to_look_back"]

                logger.info(f"🫆 Results found for {keyword} in {county_name}, {state}")
                
                # Notice processing loop
                is_processing_notices = True
                while is_processing_notices:
                    for notice_pointer in range(head_notice_node, post_tail_notice_index):
                        logger.info(f"📖 Started processing notice {notice_number} for {keyword} in {county}, {state}")

                        current_notice_publisher = executor_module.get_notice_publisher(driver, notice_pointer, metadata_timeout)

                        # No publisher metadata being found indicates that there are no more notices to 
                        # process. This occurs when there are fewer notices available than the 
                        # configured per-page display limit (i.e., notices_per_page).
                        if not current_notice_publisher:
                            logger.info(f"📕 Finished processing notice {notice_number} for {keyword} in {county}, {state} with nothing to review and no pending notices remaining")

                            # If the current keyword is entirely new 
                            # to the system then register it for tracking.
                            if is_keyword_new_to_system:
                                logger.info(f"🆕 The system has not previously registered the keyword {keyword} in {county}, {state}")
                                logger.info(f"📝 Updating the system to track {keyword} for {county}, {state}")
                                county_configs["tail_notice_metadata"][keyword]["publisher"] = head_notice_publisher
                                county_configs["tail_notice_metadata"][keyword]["publication_date"] = head_notice_publication_date
                                county_configs["tail_notice_metadata"][keyword]["excerpt"] = head_notice_excerpt
                                write_configs(county_configs, county, state, county)
                                logger.info(f"✅ {keyword} is now registered for system tracking in {county}, {state}")
                            is_processing_notices = False
                            break

                        current_notice_publication_date = executor_module.get_notice_publication_date(driver, notice_pointer, metadata_timeout)
                        current_notice_excerpt = executor_module.get_notice_excerpt(driver, notice_pointer, metadata_timeout)

                        # Cache the head notice’s metadata so it can be stored
                        # later in the county's configuration file as the new
                        # tail notice metadata once there are no more new
                        # notices to process.
                        if notice_pointer == head_notice_node and not did_current_keyword_trigger_pagination:
                            head_notice_publisher = current_notice_publisher
                            head_notice_publication_date = current_notice_publication_date
                            head_notice_excerpt = current_notice_excerpt

                        # If the current keyword is entirely new to the system and not within the lookback period then 
                        # terminate the search to prevent the extraction of stale notices. Register the current
                        # keyword for tracking.
                        if is_keyword_new_to_system and not is_notice_within_lookback_period(current_notice_publication_date, days_to_look_back):
                            logger.info(f"📕 Notice {notice_number} for {keyword} in {county}, {state} is past the lookback period leaving no pending notices")
                            logger.info(f"🆕 The system has not previously registered the keyword {keyword} in {county}, {state}")
                            logger.info(f"📝 Updating the system to track {keyword} for {county}, {state}")
                            county_configs["tail_notice_metadata"][keyword]["publisher"] = head_notice_publisher
                            county_configs["tail_notice_metadata"][keyword]["publication_date"] = head_notice_publication_date
                            county_configs["tail_notice_metadata"][keyword]["excerpt"] = head_notice_excerpt
                            write_configs(county_configs, county, state, county)
                            logger.info(f"✅ {keyword} is now registered for system tracking in {county}, {state}")
                            is_processing_notices = False
                            break

                        # Compare the current notice's metadata vs the tail notice's metadata to determine if the notice is new.
                        # Since notices are sorted from newest to oldest, the tail notice's metadata serves as the cutoff for
                        # previously seen notices. If the notice is new, continue processing. Otherwise, update the tail notice's metadata
                        # with the head notice's metadata (the new cutoff) in the county's configuration file and then
                        # move on to the next keyword search.
                        if current_notice_publisher == tail_notice_metadata["publisher"] and \
                           current_notice_publication_date == tail_notice_metadata["publication_date"] and \
                           current_notice_excerpt == tail_notice_metadata["excerpt"]:
                            logger.info(f"📕 Notice {notice_number} for {keyword} in {county}, {state} is the last entry in system tracking leaving no notices pending")
                            logger.info(f"📝 Updating tracking data for {keyword} in {county}, {state}")
                            county_configs["tail_notice_metadata"][keyword]["publisher"] = head_notice_publisher
                            county_configs["tail_notice_metadata"][keyword]["publication_date"] = head_notice_publication_date
                            county_configs["tail_notice_metadata"][keyword]["excerpt"] = head_notice_excerpt
                            write_configs(county_configs, county, state, county)
                            logger.info(f"✅ Tracking data for {keyword} updated in {county}, {state}")
                            is_processing_notices = False
                            break

                        # This interpolated variable holds the correct value for the driver to locate the
                        # view notice selector and is assigned according to the upstream `num_results`.
                        view_notice_pointer = notice_pointer + 1 if num_results <= 10 else notice_pointer

                        executor_module.view_notice(driver, view_notice_pointer, recaptcha_v2_timeout)
                        current_notice = executor_module.get_notice(driver)

                        payload_context = {
                            "publication_date": current_notice_publication_date,
                            "publisher": current_notice_publisher,
                            "text": current_notice,
                            "search_term": keyword,
                            "county": county
                        }

                        logger.info(f"👔 Started staging the payload for {target_env}")
                        payload = stage_payload(payload_context)
                        logger.info(f"🧳 Finished staging the payload for {target_env}")

                        logger.info(f"🛫 Started loading the payload to {target_env}")
                        load_payload(payload, state, target_env)
                        logger.info(f"🛬 Finished loading the payload to {target_env}")

                        executor_module.go_back_to_notice_board(driver)

                        logger.info(f"📕 Finished processing notice {notice_number} for {keyword} in {county}, {state}")
                        notice_number += 1

                        if notice_pointer != pagination_node:
                            continue

                        if executor_module.is_pagination_required(driver):
                            logger.info("🔗 Paginating")
                            executor_module.paginate(driver)
                            did_current_keyword_trigger_pagination = True
                        else:
                            is_processing_notices = False

            logger.info(f'💎 Finished extraction for {"all counties" if county == "all" else county}, {state}')
        logger.info(f'🏡 Finished dynamic driver session for {"all counties" if county == "all" else county}, {state}')
        logger.info(f'🔴 Finished extraction processing loop for {"all counties" if county == "all" else county}, {state}')

if __name__ == "__main__":
    pass