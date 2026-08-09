import unittest

from agent_bridge.cli import build_parser


class CliParserTests(unittest.TestCase):
    def test_json_flag_is_supported_after_subcommand(self) -> None:
        args = build_parser().parse_args(["doctor", "--json"])
        self.assertTrue(args.json)

    def test_db_flag_is_supported_after_subcommand(self) -> None:
        args = build_parser().parse_args(["doctor", "--db", "/tmp/custom.sqlite3"])
        self.assertEqual(args.db, "/tmp/custom.sqlite3")

    def test_anthropic_style_commands_parse(self) -> None:
        reply = build_parser().parse_args(["reply", "msg-1", "--from", "worker", "--message", "done"])
        self.assertEqual(reply.message_id, "msg-1")
        self.assertEqual(reply.from_run, "worker")
        listen = build_parser().parse_args(["listen", "worker", "--once"])
        self.assertTrue(listen.once)
        task = build_parser().parse_args(["task", "create", "--title", "inspect", "--json"])
        self.assertEqual(task.task_command, "create")
        self.assertTrue(task.json)

    def test_tell_alias_accepts_message_arguments(self) -> None:
        args = build_parser().parse_args(
            ["tell", "--from", "sender", "--to", "receiver", "--message", "hello"]
        )
        self.assertEqual(args.command, "tell")
        self.assertEqual(args.from_run, "sender")
        self.assertEqual(args.to, "receiver")


if __name__ == "__main__":
    unittest.main()
