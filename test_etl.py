import tempfile
import unittest
from pathlib import Path
from typing import Optional

import importlib_resources
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from app.etl import (
    get_spark,
    load_tickets,
    load_teams,
    load_priorities,
    join_tables,
    filter_late_tickets,
    calculate_team_scores,
    save,
)


class TestETL(unittest.TestCase):
    data_module = "test"

    @classmethod
    def setUpClass(cls) -> None:
        get_spark()
        with importlib_resources.path(cls.data_module, "tickets.jsonl") as f:
            tickets_path = Path(f)
        with importlib_resources.path(cls.data_module, "support_teams.csv") as f:
            teams_path = Path(f)
        cls.tickets = load_tickets(tickets_path)
        cls.teams = load_teams(teams_path)
        cls.priorities = load_priorities()
        cls.full_data = join_tables(cls.tickets, cls.teams, cls.priorities)

    def assertSortedListEqual(
        self, list1: list, list2: list, msg: Optional[str] = None
    ):
        self.assertListEqual(sorted(list1), sorted(list2), msg)

    def assertSetContains(self, set1: set, subset: set, msg: Optional[str] = None):
        if msg is None:
            msg = f"Set {set1} does not contain subset {subset}"
        self.assertTrue(subset.issubset(set1), msg)

    def assertDataFrameEqual(
        self, df1: DataFrame, df2: DataFrame, msg: Optional[str] = None
    ):
        self.assertEqual(
            df1.select(sorted(df1.columns))
            .exceptAll(df2.select(sorted(df2.columns)))
            .count(),
            0,
            msg,
        )

    def test_tickets_should_have_valid_number_of_rows(self):
        self.assertEqual(self.tickets.count(), 36)

    def test_tickets_should_have_valid_column_names(self):
        self.assertSortedListEqual(
            self.tickets.columns,
            [
                "ticketId",
                "teamId",
                "priorityId",
                "resolved",
                "createdAt",
                "resolvedAt",
                "responseTime",
                "satisfactionScore",
            ],
        )

    def test_teams_should_have_valid_number_of_rows(self):
        self.assertEqual(self.teams.count(), 3)

    def test_teams_should_have_valid_column_names(self):
        self.assertSortedListEqual(self.teams.columns, ["teamId", "teamName"])

    def test_priorities_should_have_valid_number_of_rows(self):
        self.assertEqual(self.priorities.count(), 3)

    def test_priorities_should_have_valid_column_names(self):
        self.assertSortedListEqual(
            self.priorities.columns, ["priorityId", "priorityName"]
        )

    def test_joined_tables_have_valid_number_of_rows(self):
        self.assertEqual(self.full_data.count(), 36)

    def test_joined_tables_have_valid_column_names(self):
        self.assertSetContains(
            set(self.full_data.columns),
            {
                "ticketId",
                "teamId",
                "teamName",
                "priorityId",
                "priorityName",
                "resolved",
                "createdAt",
                "resolvedAt",
                "responseTime",
                "satisfactionScore",
            },
        )

    def test_filtered_late_tickets_in_2_hours_have_valid_number_of_rows(self):
        valid_tickets = filter_late_tickets(self.full_data, 2)
        # Only tickets with responseTime <= 120 minutes (2 hours)
        self.assertEqual(valid_tickets.count(), 18)

    def test_filtered_late_tickets_in_2_hours_have_valid_column_names(self):
        valid_tickets = filter_late_tickets(self.full_data, 2)
        self.assertSetContains(
            set(valid_tickets.columns),
            {
                "ticketId",
                "teamId",
                "teamName",
                "priorityId",
                "priorityName",
                "resolved",
                "createdAt",
                "resolvedAt",
                "responseTime",
                "satisfactionScore",
            },
        )

    def test_filtered_late_tickets_in_3_hours_have_valid_number_of_rows(self):
        valid_tickets = filter_late_tickets(self.full_data, 3)
        # Only tickets with responseTime <= 180 minutes (3 hours)
        self.assertEqual(valid_tickets.count(), 18)

    def test_filtered_late_tickets_in_4_hours_have_valid_number_of_rows(self):
        valid_tickets = filter_late_tickets(self.full_data, 4)
        # Only tickets with responseTime <= 240 minutes (4 hours)
        self.assertEqual(valid_tickets.count(), 18)

    def test_calculated_team_scores_have_valid_number_of_rows(self):
        team_scores = calculate_team_scores(self.full_data)
        # 3 teams * 3 priorities = 9 rows
        self.assertEqual(team_scores.count(), 9)

    def test_calculated_team_scores_have_valid_columns(self):
        team_scores = calculate_team_scores(self.full_data)
        self.assertSetContains(
            set(team_scores.columns),
            {"teamId", "teamName", "priorityId", "priorityName", "finalScore"},
        )

    def test_high_priority_scores_use_max_satisfaction(self):
        team_scores = calculate_team_scores(self.full_data)
        # High priority (priorityId=1) should use max satisfaction score
        high_priority_scores = team_scores.filter(team_scores.priorityId == 1)

        # Team 1, High priority: max satisfaction should be 4
        team1_high = high_priority_scores.filter(
            high_priority_scores.teamId == 1
        ).collect()[0]
        self.assertEqual(team1_high["finalScore"], 4)

        # Team 2, High priority: max satisfaction should be 5
        team2_high = high_priority_scores.filter(
            high_priority_scores.teamId == 2
        ).collect()[0]
        self.assertEqual(team2_high["finalScore"], 5)

        # Team 3, High priority: max satisfaction should be 4
        team3_high = high_priority_scores.filter(
            high_priority_scores.teamId == 3
        ).collect()[0]
        self.assertEqual(team3_high["finalScore"], 4)

    def test_medium_priority_scores_use_max_satisfaction(self):
        team_scores = calculate_team_scores(self.full_data)
        # Medium priority (priorityId=2) should use max satisfaction score
        medium_priority_scores = team_scores.filter(team_scores.priorityId == 2)

        # Team 1, Medium priority: max satisfaction should be 3
        team1_medium = medium_priority_scores.filter(
            medium_priority_scores.teamId == 1
        ).collect()[0]
        self.assertEqual(team1_medium["finalScore"], 3)

        # Team 2, Medium priority: max satisfaction should be 4
        team2_medium = medium_priority_scores.filter(
            medium_priority_scores.teamId == 2
        ).collect()[0]
        self.assertEqual(team2_medium["finalScore"], 4)

        # Team 3, Medium priority: max satisfaction should be 3
        team3_medium = medium_priority_scores.filter(
            medium_priority_scores.teamId == 3
        ).collect()[0]
        self.assertEqual(team3_medium["finalScore"], 3)

    def test_low_priority_scores_use_min_satisfaction(self):
        team_scores = calculate_team_scores(self.full_data)
        # Low priority (priorityId=3) should use min satisfaction score
        low_priority_scores = team_scores.filter(team_scores.priorityId == 3)

        # Team 1, Low priority: min satisfaction should be 4
        team1_low = low_priority_scores.filter(
            low_priority_scores.teamId == 1
        ).collect()[0]
        self.assertEqual(team1_low["finalScore"], 4)

        # Team 2, Low priority: min satisfaction should be 4
        team2_low = low_priority_scores.filter(
            low_priority_scores.teamId == 2
        ).collect()[0]
        self.assertEqual(team2_low["finalScore"], 4)

        # Team 3, Low priority: min satisfaction should be 4
        team3_low = low_priority_scores.filter(
            low_priority_scores.teamId == 3
        ).collect()[0]
        self.assertEqual(team3_low["finalScore"], 4)

    def test_saved_data_is_in_parquet_format(self):
        valid_tickets = filter_late_tickets(self.full_data, 3)
        team_scores = calculate_team_scores(valid_tickets)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp) / "scores"
            save(team_scores, tmp_path)
            try:
                restored_scores = get_spark().read.parquet(str(tmp_path))
            except AnalysisException:
                self.fail("Stored data should not be empty")
            self.assertDataFrameEqual(team_scores, restored_scores)
            # Should NOT be partitioned by teamId
            self.assertEqual(len(list(tmp_path.glob("teamId=*"))), 0)
            # Should have parquet files directly
            self.assertTrue(len(list(tmp_path.glob("*.parquet"))) > 0)
