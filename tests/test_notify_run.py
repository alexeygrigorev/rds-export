"""Exercise the cron wrapper without contacting Relay or running a backup."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


WRAPPER = Path(__file__).resolve().parents[1] / "notify_run.sh"


class NotifyRunTests(unittest.TestCase):
    def run_wrapper(self, command, recipient=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture = root / "message.json"
            mailer = root / "datamailer"
            mailer.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, pathlib, sys\n"
                "pathlib.Path(os.environ['CAPTURE']).write_text(\n"
                "    json.dumps({'args': sys.argv[1:], 'body': sys.stdin.read()}))\n"
            )
            mailer.chmod(0o700)
            env = dict(os.environ, DATAMAILER_BIN=str(mailer), CAPTURE=str(capture))
            env.pop("BACKUP_ALERT_EMAIL", None)
            if recipient is not None:
                env["BACKUP_ALERT_EMAIL"] = recipient
            result = subprocess.run(
                ["bash", str(WRAPPER), "website-contabo", *command],
                env=env, text=True, capture_output=True, check=False,
            )
            message = json.loads(capture.read_text()) if capture.exists() else None
            return result, message

    def test_failed_backup_uses_alert_sender_and_preserves_recipient_and_exit(self):
        result, message = self.run_wrapper(
            ["bash", "-c", "echo controlled-backup-failure; exit 37"],
            "dev@dtcdev.click",
        )
        self.assertEqual(result.returncode, 37)
        self.assertIn("controlled-backup-failure", result.stdout)
        self.assertEqual(message["args"], [
            "send", "--from", "alerts", "--to", "dev@dtcdev.click",
            "--subject", "Backup failure: website-contabo (exit 37)",
        ])
        self.assertIn("controlled-backup-failure", message["body"])

    def test_default_recipient_is_unchanged(self):
        _, message = self.run_wrapper(["false"])
        args = message["args"]
        self.assertEqual(args[args.index("--to") + 1], "alexey.s.grigoriev@gmail.com")

    def test_success_streams_output_without_sending(self):
        result, message = self.run_wrapper(["echo", "backup-ok"])
        self.assertEqual(result.returncode, 0)
        self.assertIn("backup-ok", result.stdout)
        self.assertIsNone(message)


if __name__ == "__main__":
    unittest.main()
