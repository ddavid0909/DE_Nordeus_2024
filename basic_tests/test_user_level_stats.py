import unittest
import sys
import importlib
from datetime import datetime

"""
Disclaimer:
The tests given won't be used when grading the project, they are there to help you. 
These tests cover only edge cases that are found in events_test.jsonl, which is only a portion of all edge cases found in events.jsonl.
If you are not writing in python, this file can still help you as it explains events_test.jsonl and has a solution for one input (line 37).
You can completely ignore these tests if you want, no points will be deducteed.

Explanation of sample dataset:
It contains 3 registrations, 5 sessions and 1 match all in 1 day.

How to run tests:
python test_user_level_stats.py name_of_your_python_file name_of_your_function
Function should take two parameters; user_id and date
Python 3.8+ recommended
"""

class TestMyFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if len(sys.argv) < 3:
            print("Usage: python test_user_level_stats.py <module_name> <function_name>")
            sys.exit(1)

        module_name = sys.argv[1]
        function_name = sys.argv[2]

        module = importlib.import_module(module_name)
        cls.function_to_test = getattr(module, function_name)

        cls.result = cls.function_to_test(user_id="52d65a1b-8012-934e-001b-19e6ba3cdc0e", date=None)
        cls.expected_result = {
            "country": "US",
            "days_since_last_login": (datetime.today() - datetime(2024,10,9)).days,
            "registration_date_local_timezone": "2024-10-09 11:27:35",
            "number_of_sessions": 2,
            "time_spent_in_game": 780,
            "total_points_won_home": 3,
            "total_points_won_away": 0,
            "percentage_time_spent_in_match": 113/780*100
        }

    def test_country(self):
        self.assertEqual(self.result['country'], self.expected_result['country'], "Country should be 'US'")

    def test_days_since_last_login(self):
        message = f"Days since last login should be {(datetime.today() - datetime(2024, 10, 9)).days}"
        self.assertEqual(self.result['days_since_last_login'], self.expected_result['days_since_last_login'], message)

    def test_registration_date(self):
        self.assertEqual(self.result['registration_date_local_timezone'], self.expected_result['registration_date_local_timezone'], "Registration date should be '2024-10-09 11:27:35'")

    def test_number_of_sessions(self):
        self.assertEqual(self.result['number_of_sessions'], self.expected_result['number_of_sessions'], "Number of sessions should be 2")

    def test_time_spent_in_game(self):
        self.assertEqual(self.result['time_spent_in_game'], self.expected_result['time_spent_in_game'], "Time spent in game should be 780 seconds")

    def test_total_points_won_home(self):
        self.assertEqual(self.result['total_points_won_home'], self.expected_result['total_points_won_home'], "Total points won as home player should be 3")

    def test_total_points_won_away(self):
        self.assertEqual(self.result['total_points_won_away'], self.expected_result['total_points_won_away'], "Total points won as away player should be 0")
    
    #Bonus
    def test_time_spent_in_match(self):
        self.assertEqual(round(self.result['percentage_time_spent_in_match'], 2), round(self.expected_result['percentage_time_spent_in_match'], 2), f"Time percentage spent in match should be ~14.49 (%)")

if __name__ == '__main__':
    unittest.main(argv=sys.argv[:1])
