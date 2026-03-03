from lfs_sdk.envelope import is_lfs_envelope


def test_is_lfs_envelope():
    assert is_lfs_envelope(b'{"kfs_lfs":1,"bucket":"b"}')
    assert not is_lfs_envelope(b'plain')
