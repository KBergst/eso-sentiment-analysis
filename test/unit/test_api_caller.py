import pandas as pd
import pytest
import unittest.mock as mock
import sqlite3
from requests_ratelimiter import LimiterSession
import requests
import copy
from datetime import date, datetime

import eso_sentiment_analysis.api_caller as api_caller

example_json_return = [{'commentID': 497925,
  'discussionID': 61051,
  'parentRecordType': 'discussion',
  'parentRecordID': 61051,
  'name': 'Re: 2-3 second freezes',
  'categoryID': 277,
  'body': 'Awesomonium Process is the thing taking up all my CPU',
  'dateInserted': '2014-03-31T19:44:30+00:00',
  'dateUpdated': None,
  'insertUserID': 379598,
  'updateUserID': None,
  'score': 0,
  'depth': 1,
  'scoreChildComments': 0,
  'countChildComments': 0,
  'insertUser': {'userID': 379598,
   'name': 'alewis478b14_ESO',
   'url': 'https://forums.elderscrollsonline.com/profile/alewis478b14_ESO',
   'photoUrl': 'https://us.v-cdn.net/5020507/uploads/bf47ba6b81f347a352defdda0e8d80d5.png',
   'dateLastActive': '2020-06-10T04:50:20+00:00',
   'banned': 0,
   'punished': 0,
   'private': False,
   'label': '✭✭✭',
   'labelHtml': '✭✭✭'},
  'url': 'https://forums.elderscrollsonline.com/en/discussion/comment/497925#Comment_497925',
  'type': 'comment',
  'format': 'BBCode',
  'attributes': {}},
 {'commentID': 497926,
  'discussionID': 61904,
  'parentRecordType': 'discussion',
  'parentRecordID': 61904,
  'name': 'Re: Well done Zenimax',
  'categoryID': 352,
  'body': "I was a little upset when I saw some of the quest bugs found during beta hadn't been fixed...  But in general I can't complain, things seem to run better and less laggy than most MMO's that have a much longer history...",
  'dateInserted': '2014-03-31T19:44:41+00:00',
  'dateUpdated': None,
  'insertUserID': 4743204,
  'updateUserID': None,
  'score': 0,
  'depth': 1,
  'scoreChildComments': 0,
  'countChildComments': 0,
  'insertUser': {'userID': 4743204,
   'name': 'BigDumbViking',
   'url': 'https://forums.elderscrollsonline.com/profile/BigDumbViking',
   'photoUrl': 'https://us.v-cdn.net/5020507/uploads/avatarstock/nEOYDZXGZYHM2.png',
   'dateLastActive': '2014-04-25T12:28:27+00:00',
   'banned': 0,
   'punished': 0,
   'private': False,
   'label': '✭✭',
   'labelHtml': '✭✭'},
  'url': 'https://forums.elderscrollsonline.com/en/discussion/comment/497926#Comment_497926',
  'type': 'comment',
  'format': 'BBCode',
  'attributes': {}},
 {'commentID': 497927,
  'discussionID': 62938,
  'parentRecordType': 'discussion',
  'parentRecordID': 62938,
  'name': 'Re: Why limit the vanity pets and treasure maps to one per account?',
  'categoryID': 195,
  'body': 'I admit.. if I had known it was just for one character per account..  I too would have not bothered.  Not a very classy move on their part.  I hope they correct that.',
  'dateInserted': '2014-03-31T19:44:43+00:00',
  'dateUpdated': None,
  'insertUserID': 20204,
  'updateUserID': None,
  'score': 2,
  'depth': 1,
  'scoreChildComments': 0,
  'countChildComments': 0,
  'insertUser': {'userID': 20204,
   'name': 'Davorn',
   'url': 'https://forums.elderscrollsonline.com/profile/Davorn',
   'photoUrl': 'https://us.v-cdn.net/5020507/uploads/avatarstock/nALC7UYGT0FJ8.png',
   'dateLastActive': '2020-02-12T18:48:22+00:00',
   'banned': 0,
   'punished': 0,
   'private': False,
   'label': '✭✭',
   'labelHtml': '✭✭'},
  'url': 'https://forums.elderscrollsonline.com/en/discussion/comment/497927#Comment_497927',
  'type': 'comment',
  'format': 'BBCode',
  'attributes': {}}]

def test_daterange_inclusive():
    manual_daterange=[date(2015,12,30), date(2015,12,31), date(2016, 1, 1), 
                      date(2016, 1, 2), date(2016, 1, 3)]
    daterange=api_caller.daterange_inclusive(date(2015,12,30), date(2016, 1, 3))
    assert manual_daterange == list(daterange)


@pytest.mark.parametrize("test_date,db_written", [(date.today(),False), (date(2015,12,31),True), (date(2100,2,15),False)])
def test_save_completed_date(mocker, test_date, db_written):
    mocker.patch('sqlite3.connect', autospec=True)
    mock_con = sqlite3.connect("eso_forum.db")
    mock_save_to_db = mocker.patch('pandas.DataFrame.to_sql')
    str_test_date = test_date.strftime("%Y-%m-%d")
    
    api_caller._save_completed_date(mock_con,"test", str_test_date)

    if db_written:  # writes data to correct place
        mock_save_to_db.assert_called_once_with("retrieved_dates", mock_con, index=False, if_exists="append")
    else:  # doesn't write data
        mock_save_to_db.assert_not_called()
    

@pytest.mark.parametrize("json_str", [
    example_json_return,   
    [{'a':1,'image':1, 'c':1, 'd':1},{'a':1,'image':1, 'c':1, 'd':1}],
    [{'a':1,'b':1, 'image':1, 'insertUser':1},{'a':1,'b':1, 'image':1, 'insertUser':1}],
    [{'a':1,'b':1, 'c':1, 'd':1},{'a':1,'b':1, 'c':1, 'd':1}]
])
def test_save_to_table(mocker, json_str):
    mock_save_to_db = mocker.patch('pandas.DataFrame.to_sql')
    mocker.patch('sqlite3.connect', autospec=True)
    mock_con = sqlite3.connect("eso_forum.db")
    
    test_df_cols = api_caller.save_to_table(json_str, mock_con, "myTable").columns
    # successfully delete bad_fields but not throw a fit if they don't already exist
    assert 'image' not in test_df_cols
    assert 'insertUser' not in test_df_cols
    assert 'attributes' not in test_df_cols
    # saves to database and doesn't overwrite what's already there
    mock_save_to_db.assert_called_once_with("myTable", mock_con, index=False, if_exists="append")


@pytest.mark.parametrize("fields_options,expected_path",
                         [(None, 
                           "https://forums.elderscrollsonline.com/api/v2/comments?limit=100&dateInserted=2014-06-24"), 
                          ('dateInserted',
                           "https://forums.elderscrollsonline.com/api/v2/comments?limit=100&dateInserted=2014-06-24&fields=dateInserted"),
                          (['private','score'],
                           "https://forums.elderscrollsonline.com/api/v2/comments?limit=100&dateInserted=2014-06-24&fields=private,score")])
def test_get_all_from_one_day_fields(mocker, fields_options, expected_path):
    mocked_api_session = mocker.patch("requests_ratelimiter.LimiterSession", autospec=True)
    mocker.patch('pandas.DataFrame.to_sql')
    mocker.patch("eso_sentiment_analysis.api_caller.save_to_table")
    mocker.patch('sqlite3.connect', autospec=True)
    mock_con = sqlite3.connect("eso_forum.db")    
    # returns properly formatted api calling string for each field case
    api_caller.get_all_from_one_day(mocked_api_session,
                                    "https://forums.elderscrollsonline.com/api/v2/",
                                    "comments",
                                    "2014-06-24",
                                    mock_con,
                                    "myTable",
                                    specific_fields=fields_options)
    mocked_api_session.get.assert_called_with(expected_path, timeout=None)

    
@pytest.mark.parametrize("num_pages,header_dicts", 
                         [(0,[{'x-app-page-result-count':0}]),
                          (1,[{'x-app-page-result-count':1}]),
                          (6,[{'x-app-page-result-count':6,"x-app-page-next-url":"next_page"},
                              {'x-app-page-result-count':6,"x-app-page-next-url":"next_page"},
                              {'x-app-page-result-count':6,"x-app-page-next-url":"next_page"},
                              {'x-app-page-result-count':6,"x-app-page-next-url":"next_page"},
                              {'x-app-page-result-count':6,"x-app-page-next-url":"next_page"},
                              {'x-app-page-result-count':6}])])
def test_get_all_from_one_day_pages(mocker, num_pages, header_dicts):
    # cases: no data from day; one page of data from day; multiple pages of data from day
    # performs the expected loop given each case
    # saves to the database the expected number of times given each case
    mocked_api_session = mocker.patch("requests_ratelimiter.LimiterSession",
                                      new_callable=mocker.PropertyMock)
    mocked_response = mocker.patch('requests.Response', new_callable=mocker.PropertyMock)
    # generate mocked responses which exhibit necessary header qualities to mimic the api
    mocked_responses=[]
    for header in header_dicts:
        response = copy.copy(mocked_response())
        response.headers = header
        mocked_responses.append(response)
    # plug mocked responses into the get fn
    mocked_api_session.get.side_effect= mocked_responses
    mocked_save = mocker.patch("eso_sentiment_analysis.api_caller.save_to_table")
    mocked_date_save = mocker.patch("eso_sentiment_analysis.api_caller._save_completed_date")
    mocker.patch('sqlite3.connect', autospec=True)
    mock_con = sqlite3.connect("eso_forum.db")    
    api_caller.get_all_from_one_day(mocked_api_session,
                                    "https://forums.elderscrollsonline.com/api/v2/",
                                    "comments",
                                    "2014-06-24",
                                    mock_con,
                                    "myTable")
    # retrieved all pages from the api
    assert mocked_api_session.get.call_count == max(num_pages,1)
    # saved the data from each api call with some data
    assert mocked_save.call_count == num_pages
    # wrote the date once finished
    mocked_date_save.assert_called_once_with(mock_con, "comments", "2014-06-24")

    
@pytest.mark.parametrize("date_range,end_date", [(['2016-07-24', '2016-08-01'],'2016-08-01'),(['2010-01-01', '2010-01-13'],'2010-01-13'),(['2025-10-24',date.today().strftime("%Y-%m-%d")],'today')])
def test_get_all_from_endpoint(mocker, date_range, end_date):
    mocked_api_session = mocker.patch("requests_ratelimiter.LimiterSession")
    mocker.patch('sqlite3.connect', autospec=True)
    mock_con = sqlite3.connect("eso_forum.db")  
    mocked_get_from_day = mocker.patch('eso_sentiment_analysis.api_caller.get_all_from_one_day',
                                       autospec=True)
    
    # generate the expected calls to get_all_from_day
    mock_completed_dates = {"date":['2016-07-29','2004-04-04']}
    mock_get_prev_dates = mocker.patch("pandas.read_sql", autospec=True)
    mock_get_prev_dates.return_value=mock_completed_dates
    
    # call the test function
    api_caller.get_all_from_endpoint(mocked_api_session, 
                                     "https://forums.elderscrollsonline.com/api/v2/",
                          "comments", date_range[0], mock_con, "myTable", end_date=end_date,
                          record_limit=100, specific_fields='dateInserted')
    # verify correct calls were made
    expected_calls=[]
    for given_date in api_caller.daterange_inclusive(datetime.strptime(date_range[0],
                                                                       "%Y-%m-%d"),
                                          datetime.strptime(date_range[1], "%Y-%m-%d")):
        if given_date.strftime("%Y-%m-%d") not in mock_completed_dates["date"]:
            expected_calls.append(mock.call(mocked_api_session, 
                                            "https://forums.elderscrollsonline.com/api/v2/",
                              "comments", given_date.strftime("%Y-%m-%d"), mock_con, "myTable",
                              record_limit=100, specific_fields='dateInserted', saving_kwargs={}))
    mocked_get_from_day.assert_has_calls(expected_calls)
    
    