import io
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import requests_mock
from requests import session

import etoolbox.utils.remote_zip as rz


class ServerSimulator:
    def __init__(self, fname):
        self._fname = fname

    def serve(self, request, context):
        from_byte, to_byte = rz._RemoteFetcher.parse_range_header(
            request.headers["Range"]
        )
        with open(self._fname, "rb") as f:
            if from_byte < 0:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(size + from_byte, 0), 0)
                init_pos = f.tell()
                content = f.read(min(size, -from_byte))
            else:
                f.seek(from_byte, 0)
                init_pos = f.tell()
                content = f.read(to_byte - from_byte + 1)

        context.headers["Content-Range"] = rz._RemoteFetcher.build_range_header(
            init_pos, init_pos + len(content)
        )
        return content


class LocalFetcher(rz._RemoteFetcher):
    def fetch(self, data_range, stream=False):
        with open(self._url, "rb") as f:
            f.seek(0, 2)
            fsize = f.tell()

            range_min, range_max = data_range
            if range_min < 0:
                range_max = fsize - 1
                range_min = max(fsize + range_min, 0)
            elif range_max is None:
                range_max = fsize - 1

            content_range = "bytes {range_min}-{range_max}/{fsize}".format(**locals())
            f.seek(range_min, 0)

            f = io.BytesIO(f.read(range_max - range_min + 1))
            buff = rz._PartialBuffer(
                f, range_min, range_max - range_min + 1, stream=stream
            )
        return buff


class TestPartialBuffer:
    def verify(self, stream):
        pb = rz._PartialBuffer(io.BytesIO(b"aaaabbcccdd"), 10, 11, stream=stream)
        assert pb.read(5) == b"aaaab"
        assert pb.read(3) == b"bcc"
        assert pb.read(3) == b"cdd"
        pb.close()

    def test_static(self):
        self.verify(stream=False)

    def test_static_seek(self):
        pb = rz._PartialBuffer(io.BytesIO(b"aaaabbcccdd"), 10, 11, stream=False)
        assert pb.seek(10, 0) == 10
        assert pb.read(5) == b"aaaab"
        assert pb.seek(12, 0) == 12
        assert pb.read(5) == b"aabbc"
        assert pb.seek(20, 0) == 20
        assert pb.read(1) == b"d"
        assert pb.seek(10, 0) == 10
        assert pb.seek(2, 1) == 12

    def test_static_read_no_size(self):
        pb = rz._PartialBuffer(io.BytesIO(b"aaaabbcccdd"), 10, 11, stream=False)
        assert pb.read() == b"aaaabbcccdd"
        assert pb.tell() == 21
        assert pb.seek(15, 0) == 15
        assert pb.read() == b"bcccdd"
        assert pb.seek(-5, 2) == 16
        assert pb.read() == b"cccdd"
        assert pb.read() == b""

    def test_static_out_of_bound(self):
        pb = rz._PartialBuffer(io.BytesIO(b"aaaabbcccdd"), 10, 11, stream=False)
        with pytest.raises(rz.OutOfBoundError):
            pb.seek(21, 0)

    def test_stream(self):
        self.verify(stream=True)

    def test_stream_forward_seek(self):
        pb = rz._PartialBuffer(io.BytesIO(b"aaaabbcccdd"), 10, 11, stream=True)
        assert pb.seek(12, 0) == 12
        assert pb.read(3) == b"aab"
        assert pb.seek(2, 1) == 17
        assert pb.read() == b"ccdd"

        with pytest.raises(rz.OutOfBoundError):
            pb.seek(12, 0)


class TestRemoteIO:
    def fetch_fun(self, data_range, stream=False):
        # simulate 200k file
        fsize = 200 * 1024
        min_range, max_range = data_range
        if min_range < 0:
            size = -min_range
            min_range = fsize - size
        else:
            size = max_range - min_range + 1

        data = b"s" * size if stream else b"x" * size
        return rz._PartialBuffer(io.BytesIO(data), min_range, size, stream=stream)

    def test_simple(self):
        rio = rz._RemoteIO(fetch_fun=self.fetch_fun)
        assert rio._file_size is None
        rio.seek(0, 2)  # eof
        assert rio._file_size is not None
        assert rio.tell() == 200 * 1024

        curr_buffer = rio.buffer

        rio.seek(-20, 2)
        assert rio.read(2) == b"xx"
        assert rio.buffer is curr_buffer  # buffer didn't change
        assert rio.read() == b"x" * 18
        assert rio.tell() == 200 * 1024

        rio.seek(120 * 1024, 0)
        assert rio.read(2) == b"xx"
        assert rio.buffer is not curr_buffer  # buffer changed
        rio.close()

    def test_file_access(self):
        rio = rz._RemoteIO(fetch_fun=self.fetch_fun)
        rio.seek(0, 2)  # eof
        curr_buffer = rio.buffer
        # we have two file, one at pos 156879 with size 30k and the last at pos
        rio.set_position_to_size({15687: 30 * 1024, 50354: 63000})
        rio.seek(15687, 0)
        assert rio.tell() == 15687
        assert rio.read(5) == b"sssss"
        assert rio.buffer is not curr_buffer  # buffer changed
        curr_buffer = rio.buffer

        # re-read the same file
        rio.seek(15687, 0)
        assert rio.read(4) == b"ssss"
        assert repr(rio.buffer) == "<_PartialBuffer off=15687 size=30720 stream=True>"
        assert rio.buffer is not curr_buffer  # buffer changed
        curr_buffer = rio.buffer

        # move to next file
        rio.seek(50354, 0)
        assert rio.read(4) == b"ssss"
        assert repr(rio.buffer) == "<_PartialBuffer off=50354 size=63000 stream=True>"
        assert rio.buffer is not curr_buffer  # buffer changed
        curr_buffer = rio.buffer

        # seek forward
        rio.seek(60354, 0)
        assert rio.read(4) == b"ssss"
        assert rio.buffer is curr_buffer  # buffer didn't change

        # seek backward
        rio.seek(51354, 0)
        assert rio.read(4) == b"ssss"
        assert rio.buffer is not curr_buffer  # buffer changed

        rio.close()


class TestLocalFetcher:
    def test_build_range_header(self):
        header = rz._RemoteFetcher.build_range_header(0, 10)
        assert header == "bytes=0-10"

        header = rz._RemoteFetcher.build_range_header(80, None)
        assert header == "bytes=80-"

        header = rz._RemoteFetcher.build_range_header(-123, None)
        assert header == "bytes=-123"

    def test_parse_range_header(self):
        range_min, range_max = rz._RemoteFetcher.parse_range_header("bytes 0-11/12")
        assert range_min == 0
        assert range_max == 11

        range_min, range_max = rz._RemoteFetcher.parse_range_header("bytes 10-21/40")
        assert range_min == 10
        assert range_max == 21

        range_min, range_max = rz._RemoteFetcher.parse_range_header("bytes -123")
        assert range_min == -123
        assert range_max is None


def test_pairwise():
    assert list(rz._pairwise([1, 2, 3, 4])) == [(1, 2), (2, 3), (3, 4)]


class TestRemoteZip:
    @staticmethod
    def make_big_header_zip(fname, entries):
        with ZipFile(fname, "w", compression=ZIP_DEFLATED) as zipf:
            for i in range(entries):
                zipf.writestr(f"test_long_header_file_{i}", "x")

    def test_big_header(self, temp_dir):
        fname = temp_dir / "test_big_header.zip"
        self.make_big_header_zip(fname, 2000)

        with rz.RemoteZip(fname, fetcher=LocalFetcher) as zfile:
            for i, finfo in enumerate(zfile.infolist()):
                assert finfo.filename == f"test_long_header_file_{i}"
                assert finfo.file_size == 1

            assert zfile.testzip() is None

    @staticmethod
    def make_unordered_zip_file(fname):
        with ZipFile(fname, "w") as zipf:
            zipf.writestr("fileA", "A" * 300000 + "Z")
            zipf.writestr("fileB", "B" * 10000 + "Z")
            zipf.writestr("fileC", "C" * 100000 + "Z")
            info_list = zipf.infolist()
            info_list[0], info_list[1] = info_list[1], info_list[0]

    def test_unordered_fileinfo(self, temp_dir):
        """Test that zip file with unordered fileinfo records works as well. Fix #13."""
        fname = temp_dir / "test_unordered_fileinfo.zip"
        self.make_unordered_zip_file(fname)

        with rz.RemoteZip(fname, fetcher=LocalFetcher) as zfile:
            names = zfile.namelist()
            assert names == ["fileB", "fileA", "fileC"]
            with zfile.open("fileB", "r") as f:
                assert f.read() == b"B" * 10000 + b"Z"
            with zfile.open("fileA", "r") as f:
                assert f.read() == b"A" * 300000 + b"Z"
            with zfile.open("fileC", "r") as f:
                assert f.read() == b"C" * 100000 + b"Z"
            assert zfile.testzip() is None

    def test_fetch_part(self):
        # fetch a range
        expected_headers = {"Range": "bytes=10-20"}
        headers = {"Content-Range": "Bytes 10-20/30"}
        with requests_mock.Mocker() as m:
            m.register_uri(
                "GET",
                "http://test.com/file.zip",
                content=b"abc",
                status_code=200,
                headers=headers,
                request_headers=expected_headers,
            )
            fetcher = rz._RemoteFetcher("http://test.com/file.zip")
            buffer = fetcher.fetch((10, 20), stream=True)
            assert buffer.tell() == 10
            assert buffer.read(3) == b"abc"

    def test_fetch_ending(self):
        # fetch file ending
        expected_headers = {"Range": "bytes=-100"}
        headers = {"Content-Range": "Bytes 10-20/30"}
        with requests_mock.Mocker() as m:
            m.register_uri(
                "GET",
                "http://test.com/file.zip",
                content=b"abc",
                status_code=200,
                headers=headers,
                request_headers=expected_headers,
            )
            fetcher = rz._RemoteFetcher("http://test.com/file.zip")
            buffer = fetcher.fetch((-100, None), stream=True)
            assert buffer.tell() == 10
            assert buffer.read(3) == b"abc"

    @pytest.mark.skip(
        reason="('Connection broken: IncompleteRead(0 bytes read, 1000 more expected)',"
        " IncompleteRead(0 bytes read, 1000 more expected))"
    )
    def test_fetch_ending_unsupported_suffix(self):
        # fetch file ending
        expected_headers = {"Range": "bytes=900-999"}
        headers = {"Content-Range": "Bytes 900-999/1000"}
        with requests_mock.Mocker() as m:
            m.head(
                "http://test.com/file.zip",
                status_code=200,
                headers={"Content-Length": "1000"},
            )
            m.get(
                "http://test.com/file.zip",
                content=b"abc",
                status_code=200,
                headers=headers,
                request_headers=expected_headers,
            )
            fetcher = rz._RemoteFetcher(
                "http://test.com/file.zip", support_suffix_range=False
            )
            buffer = fetcher.fetch((-100, None), stream=True)
            assert buffer.tell() == 900
            assert buffer.read(3) == b"abc"

    @staticmethod
    def make_zip_file(fname):
        with ZipFile(fname, "w", compression=ZIP_DEFLATED) as zipf:
            zipf.writestr("file1", "X" + ("A" * 10000) + "Y")
            zipf.writestr("file2", "short content")
            zipf.writestr("file3", "")
            zipf.writestr("file4", "last file")

    def test_interface(self, temp_dir):
        fname = temp_dir / "test_interface.zip"
        self.make_zip_file(fname)

        zfile = rz.RemoteZip(fname, min_buffer_size=50, fetcher=LocalFetcher)
        ilist = zfile.infolist()
        assert ilist[0].filename == "file1"
        assert ilist[0].file_size == 10002
        assert zfile.read("file1") == b"X" + (b"A" * 10000) + b"Y"
        assert zfile.read("file1") == b"X" + (b"A" * 10000) + b"Y"

        assert ilist[1].filename == "file2"
        assert ilist[1].file_size == 13
        assert zfile.read("file2") == b"short content"

        assert ilist[2].filename == "file3"
        assert ilist[2].file_size == 0
        assert zfile.read("file3") == b""

        assert ilist[3].filename == "file4"
        assert ilist[3].file_size == 9
        assert zfile.read("file4") == b"last file"

        assert zfile.testzip() is None

    @pytest.mark.xfail(reason="something weird with zip64")
    def test_zip64(self):
        zfile = rz.RemoteZip(
            "test_data/zip64.zip", fetcher=LocalFetcher, allowZip64=True
        )
        assert zfile.read("big_file") == b"\x00" * (1024 * 1024)
        assert zfile.testzip() is None

    def test_range_not_supported(self):
        with requests_mock.Mocker() as m:
            m.get("http://test.com/file.zip")
            with pytest.raises(rz.RangeNotSupportedError):
                rz.RemoteZip("http://test.com/file.zip")

    def test_custom_session(self, temp_dir):
        custom_session = session()
        custom_session.headers.update({"user-token": "1234"})

        fname = temp_dir / "test_custom_session.zip"
        self.make_zip_file(fname)

        server = ServerSimulator(fname)
        expected_headers = {"user-token": "1234"}
        with requests_mock.Mocker() as m:
            m.register_uri(
                "GET",
                "http://test.com/file.zip",
                content=server.serve,
                status_code=200,
                request_headers=expected_headers,
            )
            rz.RemoteZip("http://test.com/file.zip", session=custom_session)
