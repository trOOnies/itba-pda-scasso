from code.database_funcs import parse_prev_kwargs


class TestDatabaseFuncs:
    def test_parse_prev_kwargs(self):
        with open("mentre/dags/queries/select/get_max_id.sql") as f:
            query = f.read()

        assert query == (
            """SELECT max(id) AS max_id\nFROM "{DB_SCHEMA}".{DB_TABLE};"""
        )

        res = parse_prev_kwargs(query, {"DB_SCHEMA": "mock_schema"})
        assert res == (
            """SELECT max(id) AS max_id\nFROM "mock_schema".{DB_TABLE};"""
        )

        res = parse_prev_kwargs(query, {"DB_TABLE": "mock_table"})
        assert res == (
            """SELECT max(id) AS max_id\nFROM "{DB_SCHEMA}".mock_table;"""
        )

        res = parse_prev_kwargs(query, {"DB_SCHEMA": "mock_schema", "DB_TABLE": "mock_table"})
        assert res == (
            """SELECT max(id) AS max_id\nFROM "mock_schema".mock_table;"""
        )

        res = parse_prev_kwargs(query, {"DB_TABLE": "mock_table", "DB_SCHEMA": "mock_schema"})
        assert res == (
            """SELECT max(id) AS max_id\nFROM "mock_schema".mock_table;"""
        )

        res = parse_prev_kwargs(query, {"OTHER_1": "mock_table", "DB_SCHEMA": "mock_schema"})
        assert res == (
            """SELECT max(id) AS max_id\nFROM "mock_schema".{DB_TABLE};"""
        )
