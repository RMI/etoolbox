"""Test etoolbox CLI `rmi`."""

import shutil
import sys
from pathlib import Path

import pytest

from etoolbox.utils.testing import idfn


class TestCloudEntryPoint:
    @pytest.mark.usefixtures("cloud_test_cache")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_init_dry(self, script_runner):
        """Test rmi cloud init entry point dry."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "init", "123", "-d"], print_result=True)
        assert not cloud.RMICFEZIL_TOKEN_PATH.exists()

    @pytest.mark.usefixtures("cloud_test_cache")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_init(self, script_runner):
        """Test rmi cloud init entry point."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "init", "123"], print_result=True)
        with open(cloud.RMICFEZIL_TOKEN_PATH) as f:
            assert f.read() == "123"

    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_list(self, script_runner):
        """Test rmi cloud list entry point."""
        result = script_runner.run(
            ["etb", "cloud", "list", "raw-data"], capture_output=True
        )
        assert "test_data.parquet" in result.stdout

    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_list_with_etb_entrypoint(self, script_runner):
        """Test rmi cloud list entry point."""
        result = script_runner.run(
            ["etb", "cloud", "list", "raw-data"], capture_output=True
        )
        assert "test_data.parquet" in result.stdout

    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_list_detail(self, script_runner):
        """Test rmi cloud list entry point."""
        result = script_runner.run(
            ["etb", "cloud", "list", "raw-data", "-l"], capture_output=True
        )
        assert "test_data.parquet" in result.stdout

    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_get_dest(self, script_runner, temp_dir):
        """Test rmi cloud get."""
        script_runner.run(
            [
                "etb",
                "cloud",
                "get",
                "raw-data/test_data.parquet",
                str(temp_dir / "test_data.parquet"),
            ]
        )
        assert (temp_dir / "test_data.parquet").exists()

    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_get_cwd(self, script_runner, temp_dir):
        """Test rmi cloud get."""
        cwd = Path.cwd()

        script_runner.run(["etb", "cloud", "get", "raw-data/test_data.parquet"])
        path = cwd / "test_data.parquet"
        assert path.exists()
        path.unlink()

    @pytest.mark.usefixtures("cloud_test_cache_w_files")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_init_clobber(self, script_runner):
        """Test rmi cloud init entry point clobber."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "init", "456", "-c"], print_result=True)
        with open(cloud.RMICFEZIL_TOKEN_PATH) as f:
            assert f.read() == "456"

    @pytest.mark.usefixtures("cloud_test_cache_w_files")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_clean(self, script_runner):
        """Test the rmi cloud clean entry point."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "clean"], print_result=True)
        assert not cloud.AZURE_CACHE_PATH.exists()

    @pytest.mark.usefixtures("cloud_test_cache_w_files")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_clean_dry(self, script_runner):
        """Test the rmi cloud clean entry point."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "clean", "-d"], print_result=True)
        assert cloud.AZURE_CACHE_PATH.exists()
        assert any(cloud.AZURE_CACHE_PATH.iterdir())

    @pytest.mark.usefixtures("cloud_test_cache_w_files")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_clean_all(self, script_runner):
        """Test the rmi cloud clean entry point."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "clean", "-a"], print_result=True)
        assert not cloud.AZURE_CACHE_PATH.exists()
        assert not cloud.CONFIG_PATH.exists()

    @pytest.mark.usefixtures("cloud_test_cache_w_files")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_clean_all_dry(self, script_runner):
        """Test the rmi cloud clean entry point."""
        import etoolbox.utils.cloud as cloud

        script_runner.run(["etb", "cloud", "clean", "-a", "-d"], print_result=True)
        assert cloud.AZURE_CACHE_PATH.exists()
        assert any(cloud.AZURE_CACHE_PATH.iterdir())
        assert cloud.CONFIG_PATH.exists()

    @pytest.mark.usefixtures("cloud_test_cache_w_files")
    @pytest.mark.script_launch_mode("inprocess")
    def test_cloud_cache(self, script_runner):
        """Test rmi cloud cache entry point."""
        result = script_runner.run(["etb", "cloud", "cache"], capture_output=True)
        assert "Total size:" in result.stdout


class TestPudlEntryPoint:
    @pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_table_rename_entry_point(self, script_runner, test_dir, temp_dir):
        """Test the rmi pudl rename entry point."""
        from etoolbox.utils.table_map import PUDL_TABLE_MAP

        updated = temp_dir / "_read_tables_sample.py"
        shutil.copy(test_dir / "test_data/_read_tables_sample.py", updated)

        script_runner.run(
            ["etb", "pudl", "rename", "*.py", "-y"], print_result=True, cwd=temp_dir
        )

        with open(updated) as file:
            updated_content = file.read()
        for old in PUDL_TABLE_MAP:
            assert old not in updated_content

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_clean(self, script_runner):
        """Test the rmi pudl clean entry point."""
        import etoolbox.utils.pudl as pudl

        script_runner.run(["etb", "pudl", "clean"], print_result=True)
        assert pudl.CACHE_PATH.parent.exists()
        assert not pudl.CACHE_PATH.exists()

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_cache(self, script_runner):
        """Test the rmi pudl cache entry point."""
        result = script_runner.run(["etb", "pudl", "cache"], capture_output=True)
        assert "Total size:" in result.stdout

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_clean_all(self, script_runner):
        """Test the rmi pudl clean entry point."""
        import etoolbox.utils.pudl as pudl

        script_runner.run(["etb", "pudl", "clean", "-a"], print_result=True)
        assert not pudl.CACHE_PATH.parent.exists()
        assert not pudl.TOKEN_PATH.parent.exists()

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_clean_legacy(self, script_runner):
        """Test the rmi pudl clean entry point."""
        import etoolbox.utils.pudl as pudl

        script_runner.run(["etb", "pudl", "clean", "-l"], print_result=True)
        assert not any(f for f in pudl.CACHE_PATH.parent.iterdir() if not f.is_dir())
        assert not pudl.TOKEN_PATH.parent.exists()

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_clean_dry(self, script_runner):
        """Test the rmi pudl clean entry point."""
        import etoolbox.utils.pudl as pudl

        script_runner.run(["etb", "pudl", "clean", "-d"], print_result=True)
        assert pudl.CACHE_PATH.parent.exists()
        assert pudl.CACHE_PATH.exists()

    @pytest.mark.parametrize(
        "args,expected",
        [
            ((), "nightly"),
            (("-l",), "nightly"),
            (("nightly",), "core_eia860__scd_generators.parquet"),
            (("nightly", "-l"), "core_eia860__scd_generators.parquet"),
        ],
        ids=idfn,
    )
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_list(self, script_runner, args, expected):
        """Test rmi cloud list entry point."""
        result = script_runner.run(["etb", "pudl", "list", *args], capture_output=True)
        assert expected in result.stdout

    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_get(self, script_runner, temp_dir):
        """Test rmi cloud list entry point."""
        result = script_runner.run(
            [
                "etb",
                "pudl",
                "get",
                "core_eia__codes_balancing_authority_subregions",
                "stable",
                str(temp_dir),
            ],
            capture_output=True,
        )
        assert (
            temp_dir / "core_eia__codes_balancing_authority_subregions.csv"
        ).exists()

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_clean_all_dry(self, script_runner):
        """Test the rmi pudl clean entry point."""
        import etoolbox.utils.pudl as pudl

        script_runner.run(["etb", "pudl", "clean", "-a", "-d"], print_result=True)
        assert pudl.CACHE_PATH.parent.exists()
        assert pudl.TOKEN_PATH.parent.exists()

    @pytest.mark.usefixtures("pudl_test_cache_for_ep")
    @pytest.mark.script_launch_mode("inprocess")
    def test_pudl_clean_legacy_dry(self, script_runner):
        """Test the rmi pudl clean entry point."""
        import etoolbox.utils.pudl as pudl

        script_runner.run(["etb", "pudl", "clean", "-l", "-d"], print_result=True)
        assert any(f for f in pudl.CACHE_PATH.parent.iterdir() if not f.is_dir())
        assert pudl.TOKEN_PATH.parent.exists()
