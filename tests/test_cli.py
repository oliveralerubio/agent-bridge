import contextlib
import io
import unittest

from agent_bridge.cli import build_parser


class CliParserTests(unittest.TestCase):
    def test_version_flag_is_available(self) -> None:
        parser = build_parser()
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            parser.parse_args(["--version"])
        self.assertEqual(raised.exception.code, 0)
        self.assertEqual(output.getvalue().strip(), "0.4.0")

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

    def test_extended_commands_accept_json_after_leaf(self) -> None:
        team = build_parser().parse_args(["team", "status", "reviewers", "--json"])
        self.assertTrue(team.json)
        report = build_parser().parse_args(["task", "complete", "task-1", "--run", "worker", "--summary-file", "summary.json", "--json"])
        self.assertEqual(report.summary_file, "summary.json")
        self.assertTrue(report.json)

    def test_run_is_a_top_level_manifest_command(self) -> None:
        args = build_parser().parse_args(["run", "--manifest", "execution.json", "--resume", "--json"])
        self.assertEqual(args.manifest, "execution.json")
        self.assertTrue(args.resume)
        self.assertTrue(args.json)

    def test_execution_inspection_commands_are_nested(self) -> None:
        args = build_parser().parse_args(["execution", "show", "exec-1", "--json"])
        self.assertEqual(args.execution_command, "show")
        self.assertEqual(args.execution, "exec-1")
        self.assertTrue(args.json)
        stop = build_parser().parse_args(["execution", "stop", "exec-1", "--json"])
        self.assertEqual(stop.execution_command, "stop")


if __name__ == "__main__":
    unittest.main()
