"""
Functions involved with calling the ESO forum api.
"""

import argparse
from datetime import date, datetime, timedelta

import pandas as pd
import sqlite3
from requests.adapters import HTTPAdapter
from requests_ratelimiter import LimiterSession
from urllib3.util import Retry


def daterange_inclusive(start_date: date, end_date: date):
    """
    adapted from https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python to be inclusive
    You can tell it isn't my code bc they actually use type hinting B)
    I should really do that....
    """
    days = int((end_date - start_date).days) + 1
    for n in range(days):
        yield start_date + timedelta(n)


def generate_session(
    per_minute_limit=100,
    num_retries=10,
    status=5,
    status_forcelist=[504],
    backoff_factor=0.5,
    raise_on_status=True,
):
    """Create the api session used to make requests. Implements rate limits
    and retries on timeout errors.
    """
    session = LimiterSession(per_minute=per_minute_limit)
    retry_obj = Retry(
        total=num_retries,
        status=status,
        status_forcelist=status_forcelist,
        backoff_factor=backoff_factor,
        raise_on_status=raise_on_status,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry_obj))

    return session


def save_to_table(
    json_response,
    destination_db_con,
    destination_table_name,
    bad_fields=["image", "insertUser", "attributes"],
):
    """saves json to table in database, minus some fields"""
    resp_df = pd.DataFrame(json_response)
    bad_filter = resp_df.filter(bad_fields)
    resp_df = resp_df.drop(bad_filter, axis="columns")
    resp_df.to_sql(
        destination_table_name, destination_db_con, index=False, if_exists="append"
    )

    return resp_df


def _save_completed_date(
    destination_db_con, endpoint, fin_date
):
    """
    writes a date and endpoint to a table in a database. For use by get_all_from_one_day
    once all the data from the given day has been saved
    """
    # don't save if today bc not finished for sure
    if date.today() <= datetime.strptime(fin_date, "%Y-%m-%d").date():
        print("not marking present or future date complete; data may be incomplete")
        return
    date_df = pd.DataFrame({"endpoint": [endpoint], "date": [fin_date]})
    date_df.to_sql(
        "retrieved_dates", destination_db_con, index=False, if_exists="append"
    )


def get_all_from_one_day(
    api_session,
    api_base_url,
    endpoint,
    date,
    destination_db_con,
    destination_table_name,
    record_limit=100,
    specific_fields=None,
    saving_kwargs={},
):
    """get all from endpoint on a given day"""
    # TODO HANDLING TIMEOUTS error 504
    # handle field spec cases
    fields_string = ""
    if not hasattr(specific_fields, "__len__") and (specific_fields is None):
        pass  # no fields specified
    elif isinstance(specific_fields, str):
        # one field specified as string
        fields_string = "&fields=" + specific_fields
    else:  # list of fields
        fields_string = "&fields=" + ",".join(specific_fields)
    # pull the data

    # get the first page and send it
    response = api_session.get(
        api_base_url
        + endpoint
        + "?"
        + f"limit={record_limit}"
        + f"&dateInserted={date}"
        + fields_string,
        timeout=None,
    )
    if response.headers["x-app-page-result-count"] == 0:
        # no data for this day
        _save_completed_date(
            destination_db_con, endpoint, date
        )
        return

    save_to_table(
        response.json(), destination_db_con, destination_table_name, **saving_kwargs
    )
    print(response.headers.keys())
    # get the remaining pages
    while "x-app-page-next-url" in response.headers.keys():
        print("in while loop")
        response = api_session.get(
            response.headers["x-app-page-next-url"], timeout=None
        )
        save_to_table(
            response.json(), destination_db_con, destination_table_name, **saving_kwargs
        )
    # save date to 'completed' table
    _save_completed_date(
        destination_db_con, endpoint, date
    )


def get_all_from_endpoint(
    api_session,
    api_base_url,
    endpoint,
    start_date,
    destination_db_con,
    destination_table_name,
    end_date="today",
    record_limit=100,
    specific_fields=None,
    saving_kwargs={},
):
    """
    start_date: string
        use YYYY-MM-DD format (RFC3339)
    record_limit: int, default 100
        the max number of records to return per api call
    specific_fields: list-like or None, default None
        list (or tuple or whatever) of names of specific fields to request from the api
    """
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    # grab today's date if necessary
    if end_date == "today":
        end_date_dt = date.today()
        end_date = end_date_dt.strftime("%Y-%m-%d")
    else:
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    # pull previously finished dates
    completed_dates = pd.read_sql(
        "SELECT date FROM retrieved_dates WHERE endpoint=?",
        destination_db_con,
        params=[endpoint,],
    )

    print(completed_dates)

    for given_date_dt in daterange_inclusive(start_date_dt, end_date_dt):
        given_date = given_date_dt.strftime("%Y-%m-%d")
        if given_date in completed_dates["date"]:
            print(
                f"data from {given_date} on {endpoint} endpoint already retrieved, skipping"
            )
            continue
        print(f"retrieving data from {given_date}...")
        get_all_from_one_day(
            api_session,
            api_base_url,
            endpoint,
            given_date,
            destination_db_con,
            destination_table_name,
            record_limit=record_limit,
            specific_fields=specific_fields,
            saving_kwargs=saving_kwargs,
        )


############ script part ##################
if __name__ == "__main__":
    # get command-line options
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "endpoint", help="the api endpoint desired e.g. comments, discussions"
    )
    parser.add_argument(
        "--start_date",
        help="the earliest date from which you want to pull records",
        default="2014-01-01",
    )
    parser.add_argument(
        "--end_date",
        help="the latest date from which you want to pull records",
        default="today",
    )
    args = parser.parse_args()
    # do default connections
    api_session = generate_session()
    db_con = sqlite3.connect("data/eso_forum.db")
    # run dat boi
    get_all_from_endpoint(
        api_session,
        "https://forums.elderscrollsonline.com/api/v2/",
        args.endpoint,
        args.start_date,
        db_con,
        args.endpoint,
        end_date=args.end_date,
    )
