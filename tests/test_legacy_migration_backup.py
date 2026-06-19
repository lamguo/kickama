import importlib.util
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "legacy_migration", ROOT / "tools" / "legacy_migration.py"
)
legacy_migration = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(legacy_migration)


def migration_config(migration_id, backup_dir):
    return legacy_migration.MigrationConfig(
        migration_id=migration_id,
        migration_type=legacy_migration.MigrationType.DATA,
        from_version=1,
        to_version=2,
        source_connection="source",
        target_connection="target",
        backup_dir=backup_dir,
    )


class LegacyMigrationBackupTests(TestCase):
    def test_rollback_missing_backup_returns_failed_result(self):
        with TemporaryDirectory() as tmp:
            engine = legacy_migration.MigrationEngine(
                migration_config("MIG-MISSING", tmp)
            )

            with self.assertLogs(legacy_migration.logger, level="ERROR") as logs:
                result = engine.rollback()

        self.assertEqual(result.status, legacy_migration.MigrationStatus.FAILED)
        self.assertIn("No backup found", result.errors[0]["error"])
        self.assertTrue(any("No backup found" in line for line in logs.output))

    def test_rollback_missing_manifest_does_not_report_success(self):
        with TemporaryDirectory() as tmp:
            backup_path = Path(tmp) / "migration_MIG-NO-MANIFEST"
            backup_path.mkdir()
            engine = legacy_migration.MigrationEngine(
                migration_config("MIG-NO-MANIFEST", tmp)
            )

            result = engine.rollback()

        self.assertEqual(result.status, legacy_migration.MigrationStatus.FAILED)
        self.assertIn("Backup restore failed", result.errors[0]["error"])

    def test_restore_handles_manifest_read_oserror(self):
        with TemporaryDirectory() as tmp:
            backup_path = Path(tmp) / "migration_MIG-READ"
            backup_path.mkdir()
            (backup_path / "manifest.json").write_text("{}", encoding="utf-8")
            engine = legacy_migration.MigrationEngine(
                migration_config("MIG-READ", tmp)
            )

            with patch("builtins.open", side_effect=OSError("permission denied")):
                with self.assertLogs(legacy_migration.logger, level="ERROR") as logs:
                    restored = engine._restore_from_backup(backup_path)

        self.assertFalse(restored)
        self.assertTrue(any("Cannot read backup" in line for line in logs.output))

    def test_rollback_cli_returns_nonzero_for_missing_backup(self):
        with TemporaryDirectory() as tmp:
            argv = [
                "legacy_migration.py",
                "rollback",
                "--migration-id",
                "MIG-CLI-MISSING",
                "--backup-dir",
                tmp,
            ]

            with patch.object(sys, "argv", argv):
                exit_code = legacy_migration.main()

        self.assertEqual(exit_code, 2)

    def test_rollback_valid_manifest_reports_rolled_back(self):
        with TemporaryDirectory() as tmp:
            backup_path = Path(tmp) / "migration_MIG-OK"
            backup_path.mkdir()
            manifest = {"created_at": "2024-01-01T00:00:00+00:00"}
            (backup_path / "manifest.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )
            engine = legacy_migration.MigrationEngine(
                migration_config("MIG-OK", tmp)
            )

            result = engine.rollback()

        self.assertEqual(result.status, legacy_migration.MigrationStatus.ROLLED_BACK)
