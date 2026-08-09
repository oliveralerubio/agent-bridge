import unittest

from agent_bridge.cli import build_parser


class CliParserTests(unittest.TestCase):
    def test_json_flag_is_supported_after_subcommand(self) -> None:
        args = build_parser().parse_args(["doctor", "--json"])
        self.assertTrue(args.json)

    def test_db_flag_is_supported_after_subcommand(self) -> None:
        args = build_parser().parse_args(["doctor", "--db", "/tmp/custom.sqlite3"])
        self.assertEqual(args.db, "/tmp/custom.sqlite3")

    def test_tell_alias_accepts_message_arguments(self) -> None:
        args = build_parser().parse_args(
            ["tell", "--from", "sender", "--to", "receiver", "--message", "hello"]
        )
        self.assertEqual(args.command, "tell")
        self.assertEqual(args.from_run, "sender")
        self.assertEqual(args.to, "receiver")


if __name__ == "__main__":
    unittest.main()
