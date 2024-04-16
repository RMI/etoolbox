import datetime as dt
import json
import logging
import time

from etoolbox.utils.logging import JSONFormatter


def test_json_formatter():
    """Test JSONFormatter."""
    fmter = JSONFormatter()
    log_record = logging.LogRecord("test", logging.INFO, "test", 0, "test", None, None)
    msg = fmter.format(log_record)
    assert msg == json.dumps(
        {
            "message": "test",
            "timestamp": dt.datetime.fromtimestamp(
                log_record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
    )


class TestSetupLogging:
    """Test setup_logging."""

    def test_basic_logging(self, test_logger):
        """Test setup_logging."""
        logger, log_file = test_logger
        logger.info("test")
        time.sleep(0.1)
        with open(log_file) as f:
            msg = json.loads(f.readlines()[-1])
        assert msg["message"] == "test"

    def test_extra_logging(self, test_logger):
        """Test setup_logging."""
        logger, log_file = test_logger
        logger.info("test", extra={"foo": "bar"})
        time.sleep(0.1)
        with open(log_file) as f:
            msg = json.loads(f.readlines()[-1])
        assert msg["foo"] == "bar"

    def test_exception_logging(self, test_logger):
        """Test setup_logging."""
        logger, log_file = test_logger
        try:
            raise ValueError
        except ValueError as exc:
            logger.error("exception test", exc_info=exc)
        time.sleep(0.1)
        with open(log_file) as f:
            msg = json.loads(f.readlines()[-1])
        assert "Traceback (most recent call last):" in msg["message"]
