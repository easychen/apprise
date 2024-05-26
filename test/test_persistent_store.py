# -*- coding: utf-8 -*-
# BSD 2-Clause License
#
# Apprise - Push Notification Library.
# Copyright (c) 2024, Chris Caron <lead2gold@gmail.com>
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import os
import pytest
import json
from freezegun import freeze_time
from unittest import mock
from apprise import AppriseAsset
from datetime import datetime, timedelta
from apprise.persistent_store import (
    CacheJSONEncoder, CacheObject, PersistentStore, PersistentStoreMode)

# Disable logging for a cleaner testing output
import logging
logging.disable(logging.CRITICAL)


def test_disabled_persistent_storage(tmpdir):
    """
    Persistent Storage General Testing

    """
    # Our asset objecet
    asset = AppriseAsset(persistent_storage=False)

    # Create ourselves an attachment object
    pc = PersistentStore(namespace='abc', path=str(tmpdir), asset=asset)
    assert pc.read() is None
    assert pc.write('data') is False
    assert pc.get('key') is None
    assert pc.set('key', 'value') is False

    # After all of the above, nothing was done to the directory
    assert len(os.listdir(str(tmpdir))) == 0


def test_persistent_storage_init(tmpdir):
    """
    Test storage initialization
    """
    # Our asset objecet
    asset = AppriseAsset(persistent_storage=True)

    with pytest.raises(AttributeError):
        PersistentStore(namespace="", path=str(tmpdir), asset=asset)
    with pytest.raises(AttributeError):
        PersistentStore(namespace=None, path=str(tmpdir), asset=asset)

    with pytest.raises(AttributeError):
        PersistentStore(namespace="_", path=str(tmpdir), asset=asset)
    with pytest.raises(AttributeError):
        PersistentStore(namespace=".", path=str(tmpdir), asset=asset)
    with pytest.raises(AttributeError):
        PersistentStore(namespace="-", path=str(tmpdir), asset=asset)

    with pytest.raises(AttributeError):
        PersistentStore(namespace="_abc", path=str(tmpdir), asset=asset)
    with pytest.raises(AttributeError):
        PersistentStore(namespace=".abc", path=str(tmpdir), asset=asset)
    with pytest.raises(AttributeError):
        PersistentStore(namespace="-abc", path=str(tmpdir), asset=asset)

    with pytest.raises(AttributeError):
        PersistentStore(namespace="%", path=str(tmpdir), asset=asset)


def test_persistent_storage_force_method(tmpdir):
    """
    Persistent Storage General Testing

    """
    # Our asset objecet
    asset = AppriseAsset(persistent_storage=True)

    namespace = 'abc'
    # Create ourselves an attachment object
    pc = PersistentStore(
        namespace=namespace, path=str(tmpdir),
        method=PersistentStoreMode.FORCE, asset=asset)

    assert pc.size() == 0

    # Key is not set yet
    assert pc.get('key') is None
    assert 'key' not in pc

    # Verify our data is set
    assert pc.set('key', 'value')
    assert pc.size() > 0

    # Setting the same value again uses a lazy mode and
    # bypasses all of the write overhead
    assert pc.set('key', 'value')

    # Now our key is set
    assert 'key' in pc
    assert pc.get('key') == 'value'

    # A directory was created identified by the namespace
    assert len(os.listdir(str(tmpdir))) == 1
    assert namespace in os.listdir(str(tmpdir))

    path = os.path.join(str(tmpdir), namespace)
    path_content = os.listdir(path)
    assert len(path_content) == 2

    # Our temporary directory used for all file handling in this namespace
    assert '.tmp' in path_content
    # Our cache file
    assert PersistentStore.cache_file in path_content

    path = os.path.join(path, '.tmp')
    path_content = os.listdir(path)

    # We always do our best to clean any temporary files up
    assert len(path_content) == 0

    # Destroy our object
    del pc

    # Re-initialize it
    pc = PersistentStore(
        namespace=namespace, path=str(tmpdir),
        method=PersistentStoreMode.FORCE, asset=asset)

    # Our key is persistent and available right away
    assert pc.get('key') == 'value'
    assert 'key' in pc

    # Remove our item
    del pc['key']
    assert pc.size() == 0
    assert 'key' not in pc


def test_persistent_storage_cache_io_errors(tmpdir):
    """
    Test persistent storage when there is a variety of disk issues
    """

    # Our asset objecet
    asset = AppriseAsset(persistent_storage=True)

    # Namespace
    namespace = 'abc123'

    with mock.patch('gzip.open', side_effect=OSError()):
        pc = PersistentStore(
            namespace=namespace, path=str(tmpdir), asset=asset)

        # Falls to default
        assert pc.get('key') is None

        with pytest.raises(KeyError):
            pc['key']


def test_persistent_storage_cache_object(tmpdir):
    """
    Test our cache object
    """

    # A cache object
    c = CacheObject(123)
    assert c

    # A cache object that expires in 30 seconds from now
    c = CacheObject(123, 30)
    assert c
    with freeze_time(datetime.now() + timedelta(seconds=31)):
        # Our object has expired
        assert not c

    # Freeze our time for accurate testing:
    with freeze_time(datetime(2024, 5, 26, 12, 0, 0, 0)):

        # freeze_gun doesn't support non-naive timezones
        EPOCH = datetime(1970, 1, 1)

        # test all of our supported types
        for entry in ('string', 123, 1.2222, datetime.now(), None, False,
                      True):
            # Create a cache object that expires tomorrow
            c = CacheObject(entry, datetime.now() + timedelta(days=1))

            # Verify our content hasn't expired
            assert c

            # Verify we can dump our object
            result = json.loads(json.dumps(
                c, separators=(',', ':'), cls=CacheJSONEncoder))

            # Instantiate our object
            cc = CacheObject.instantiate(result)
            assert cc.json() == c.json()

        assert CacheObject.instantiate(None) is None
        assert CacheObject.instantiate({}) is None

        # Bad data
        assert CacheObject.instantiate({
            'v': 123,
            'x': datetime.now(),
            'c': 'int'}) is None

        assert CacheObject.instantiate({
            'v': 123,
            'x': (datetime.now() - EPOCH).total_seconds(),
            'c': object}) is None

        assert CacheObject.instantiate({
            'v': 123,
            'x': (datetime.now() - EPOCH).total_seconds(),
            'm': object}) is None

        obj = CacheObject.instantiate({
            'v': 123,
            'x': (datetime.now() - EPOCH).total_seconds(),
            'c': 'int'}, verify=False)
        assert isinstance(obj, CacheObject)
        assert obj == 123

        # no MD5SUM and verify is set to true
        assert CacheObject.instantiate({
            'v': 123,
            'x': (datetime.now() - EPOCH).total_seconds(),
            'c': 'int'}, verify=True) is None
