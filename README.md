# Support-ticket-analysis
# Support Ticket Analysis

This project is a technical assessment that tests your ability to work with PySpark and data transformation. You will be implementing an ETL pipeline to process customer support ticket data.

## Project Structure

```
.
├── app/
│   └── etl.py           # Your implementation goes here
├── test/
│   └── test_etl.py      # Test framework
└── verify_pack/         # Verification data and tests
    ├── support_teams.csv  # Support team metadata
    ├── tickets.jsonl    # Support ticket data
    └── test_etl.py     # Verification tests
```

## Data Description

The project works with three types of data:

1. **Support Tickets** (`tickets.jsonl`):
   - Contains customer support ticket data
   - Each ticket entry includes:
     - `ticketId`: Unique identifier
     - `teamId`: Support team ID
     - `priorityId`: Priority level (1=High, 2=Medium, 3=Low)
     - `resolved`: Whether the ticket was resolved
     - `createdAt`: When the ticket was created
     - `resolvedAt`: When the ticket was resolved
     - `responseTime`: Time to resolution in minutes
     - `satisfactionScore`: Customer satisfaction score (1-5)

2. **Support Teams** (`support_teams.csv`):
   - Contains team metadata
   - Each team has:
     - `teamId`: Team ID
     - `teamName`: Team name

3. **Priorities** (hardcoded in `load_priorities()`):
   - Maps priority IDs to names
   - Current priorities:
     - 1: High
     - 2: Medium
     - 3: Low

## Tasks

You need to implement the following functions in `app/etl.py`:

1. `load_tickets(tickets_path: Path) -> DataFrame`:
   - Load and parse the tickets from JSONL file
   - Return a DataFrame with the correct schema

2. `load_teams(teams_path: Path) -> DataFrame`:
   - Load and parse the teams from CSV file
   - Return a DataFrame with the correct schema

3. `load_priorities() -> DataFrame`:
   - Return a DataFrame with priority ID to name mapping
   - Use the priorities described above

4. `join_tables(tickets: DataFrame, teams: DataFrame, priorities: DataFrame) -> DataFrame`:
   - Join the three tables to create a complete view of the data
   - Include all relevant columns

5. `filter_late_tickets(data: DataFrame, hours: int) -> DataFrame`:
   - Filter out tickets that were resolved more than specified hours after creation
   - Only include resolved tickets
   - Return filtered DataFrame

6. `calculate_team_scores(data: DataFrame) -> DataFrame`:
   - Calculate final scores for each team and priority
   - For each team and priority:
     - If priority is High or Medium, use max satisfaction score
     - If priority is Low, use min satisfaction score
   - Return DataFrame with scores

7. `save(data: DataFrame, output_path: Path) -> None`:
   - Save the DataFrame to the specified path
   - Use Parquet format

## Requirements

- Python 3.8+
- PySpark 3.5.0+
- The code should be well-documented and type-hinted
- All functions should handle errors appropriately
- The implementation should be efficient and follow PySpark best practices

## Running the Tests

1. Install dependencies:
```bash
pip install -e .
```

2. Run the tests:
```bash
pytest test/
```

3. Verify your implementation:
```bash
pytest verify_pack/
```

## Notes

- The test data includes various edge cases and timing patterns
- Some tickets are marked as unresolved and should be filtered out
- The timing patterns in the tickets test your ability to work with datetime operations
- The score calculations test your understanding of different aggregation strategies

## Hints

To execute the unit tests, use:

```
pip install -q -e . && python3 setup.py pytest
```
