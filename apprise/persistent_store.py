# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 Chris Caron <lead2gold@gmail.com>
# All rights reserved.
#
# This code is licensed under the MIT License.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files(the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions :
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
import os
import re
import gzip
import glob
import tempfile
import json
import platform
from datetime import datetime, timezone, timedelta
from .asset import AppriseAsset
import hashlib
from .logger import logger

# Used for writing/reading time stored in cache file
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class PersistentStoreMode:
    # Flush occasionally to disk and always after object is
    # closed.
    AUTO = 'auto'

    # Always flush every change to disk after it's saved
    FORCE = 'force'

    # memory based store only
    NEVER = 'memory'


PERSISTENT_STORE_MODES = (
    PersistentStoreMode.AUTO,
    PersistentStoreMode.FORCE,
    PersistentStoreMode.NEVER,
)


class CacheObject:

    def __init__(self, value=None, expires=None):
        """
        Tracks our objects and associates a time limit with them
        """

        self.__value = value
        self.__class_name = value.__class__.__name__
        self.__expires = None
        self.__value = value
        self.set_expiry(expires)

    def set_expiry(self, expires=None):
        """
        Sets a new expirty
        """

        if isinstance(expires, (float, int)):
            self.__expires = \
                datetime.now(tz=timezone.utc) + timedelta(seconds=expires)

        elif isinstance(expires, datetime):
            self.__expires = expires.astimezone(timezone.utc)

        elif expires is None:
            # Accepted - no expiry
            self.__expires = None

        else:  # Unsupported
            raise AttributeError(
                f"An invalid expiry time ({expires} was specified")

    def __assign__(self, value):
        """
        Assigns a value without altering it's expiry
        """
        self.__value = value
        self.__class_name = value.__class__.__name__

    def __bool__(self):
        """
        Returns True it the object hasn't expired, and False if it has
        """
        if self.__expires is None:
            # No Expiry
            return True

        # Calculate if we've expired or not
        return self.__expires >= datetime.now(tz=timezone.utc)

    def md5(self):
        """
        Our checksum to track the validity of our data
        """
        return hashlib.md5(
            str(self).encode('utf-8'), usedforsecurity=False).hexdigest()

    def json(self):
        """
        Returns our preparable json object
        """
        return {
            'v': self.__value,
            'x': (self.__expires - EPOCH).total_seconds()
            if self.__expires else None,
            'c': self.__class_name,
            'm': self.md5()[:6],
        }

    @staticmethod
    def instantiate(content, verify=True):
        """
        Loads back data read in and returns a CacheObject or None if it could
        not be loaded. You can pass in the contents of CacheObject.json() and
        you'll receive a copy assuming the md5 checks okay

        """
        try:
            value = content['v']
            expires = content['x']
            if expires is not None:
                expires = datetime.fromtimestamp(expires, timezone.utc)

            # Acquire some useful integrity objects
            class_name = content.get('c', '')
            if not isinstance(class_name, str):
                raise TypeError('Class name not expected string')
            md5sum = content.get('m', '')
            if not isinstance(md5sum, str):
                raise TypeError('MD5SUM not expected string')

        except (TypeError, KeyError) as e:
            logger.trace(f'CacheObject could not be parsed from {content}')
            logger.trace('CacheObject exception: %s' % str(e))
            return None

        if class_name in ('datetime', 'FakeDatetime'):
            # Note: FakeDatetime comes from our test cases so that we can still
            #       verify this code execute sokay

            # Convert our object back to a datetime object
            value = datetime.fromisoformat(value)

        # Initialize our object
        try:
            co = CacheObject(value, expires)

        except AttributeError:
            logger.trace(f'CacheObject could not be initialied from {content}')

        if verify and co.md5()[:6] != md5sum:
            # Our object was tampered with
            logger.debug(f'Tampering detected with cache entry {co}')
            del co
            return None

        return co

    @property
    def value(self):
        """
        Returns our value
        """
        return self.__value

    @property
    def expiry(self):
        """
        Returns the number of seconds from now the object will expiry
        """
        return None if self.__expires is None else \
            (self.__expires - datetime.now(tz=timezone.utc))\
            .total_seconds()

    def __eq__(self, other):
        """
        Handles equality == flag
        """
        return self and self.__value == other

    def __str__(self):
        """
        string output of our data
        """
        return f'{self.__class_name}:{self.__value} expires: ' +\
            'never' or self.__expires.isoformat()


class CacheJSONEncoder(json.JSONEncoder):
    """
    A JSON Encoder for handling each of our cache objects
    """

    def default(self, entry):
        if isinstance(entry, datetime):
            return entry.isoformat()

        if isinstance(entry, CacheObject):
            return entry.json()
        return super().default(entry)


class PersistentStore:
    """
    An object to make working with persistent storage easier

    read() and write() are used for direct file i/o

    set(), get() are used for caching
    """

    # The maximum file-size we will allow the persistent store to grow to
    # 1 MB = 1048576 bytes
    max_file_size = 1048576

    # File encoding to use
    encoding = 'utf-8'

    # Default data set
    base_key = 'default'

    # Directory to store cache
    cache_file = '_cache.dat'

    # backup cache file (prior to being removed completely)
    cache_file_backup = '_cache.bak'

    # Our Temporary working directory
    temp_dir = '.tmp'

    # Used to verify the token specified is valid
    #  - must start with an alpha_numeric
    #  - following optional characters can include period, underscore and
    #    equal
    __valid_token = re.compile(r'[a-z0-9][a-z0-9._=]*', re.I)

    def __init__(self, namespace, path=None, method=None, asset=None):
        """
        Provide the namespace to work within. namespaces can only contain
        alpha-numeric characters with the exception of '-' (dash), '_'
        (underscore), and '.' (period). The namespace must be be relative
        to the current URL being controlled.

        The path identifies the base persistent store directory content is
        referenced to/from.

        The asset object contains switches to additionally control the
        operation of this object.
        """

        # Populated only once and after size() is called
        self.__exclude_size_list = None

        if not isinstance(namespace, str) \
                or not self.__valid_token.match(namespace):
            raise AttributeError(
                f"Persistent Storage namespace ({namespace}) provided is"
                " invalid")

        # Assign our asset
        self.asset = \
            asset if isinstance(asset, AppriseAsset) else AppriseAsset()

        if method is None:
            # Store Default
            self.method = PERSISTENT_STORE_MODES[0]

        elif method not in PERSISTENT_STORE_MODES:
            raise AttributeError(
                f"Persistent Storage mode ({method}) provided is"
                " invalid")

        # Tracks when we have content to flush
        self.__dirty = False

        # Store our method
        self.method = method

        # Prepare our path
        if path is None:
            path = os.path.join(
                os.path.normpath(
                    os.path.expandvars('%APPDATA%/Apprise/ps/')
                    if platform.system() == 'Windows'
                    else os.path.expanduser('~/.local/share/apprise/ps/')),
                namespace)

        else:
            self.__base_path = os.path.normpath(os.path.expanduser(path))

        # Our directories
        self.__base_path = os.path.join(path, namespace)
        self.__temp_path = os.path.join(self.__base_path, self.temp_dir)

        # A caching value to track persistent storage disk size
        self.__size = None

        # Internal Cache
        self._cache = None

    def read(self, key=None):
        """
        Returns the content of the persistent store object
        """

        if key is None:
            key = self.base_key

        elif not isinstance(key, str) or not self.__valid_token.match(key):
            raise AttributeError(
                f"Persistent Storage key ({key} provided is invalid")

        if not self.asset.persistent_storage:
            return None

        # generate our filename
        io_file = os.path.join(self.__base_path, f"{key}.dat")

        try:
            with open(io_file, mode="rb") as fd:
                return fd.read(self.max_file_size)

        except (OSError, IOError):
            # We can't access the file or it does not exist
            pass

        # return none
        return None

    def write(self, data, key=None):
        """
        Writes the content to the persistent store if it doesn't exceed our
        filesize limit.
        """

        if not self.asset.persistent_storage:
            return False

        if key is None:
            key = self.base_key

        elif not isinstance(key, str) or not self.__valid_token.match(key):
            raise AttributeError(
                f"Persistent Storage key ({key} provided is invalid")

        # generate our filename
        io_file = os.path.join(self.__base_path, f"{key}.dat")

        if (len(data) + self.size(exclude=key)) > self.max_file_size:
            # The content to store is to large
            logger.error(
                'Content exceeds allowable maximum file length '
                '({}KB): {}'.format(
                    int(self.max_file_size / 1024), self.url(privacy=True)))
            return False

        # ntf = NamedTemporaryFile
        ntf = None
        try:
            ntf = tempfile.NamedTemporaryFile(
                mode="w+", encoding=self.encoding, dir=self.__temp_path,
                delete=False)

            # Write our content
            ntf.write(data)

        except (OSError, IOError):
            # We can't access the file or it does not exist
            if ntf:
                try:
                    ntf.close()
                except Exception:
                    logger.trace(
                        f'Could not close() persistent content {ntf.name}')
                    pass

                try:
                    ntf.unlink(ntf.name)

                except Exception:
                    logger.error(
                        f'Could not remove persistent content {ntf.name}')

            return False

        try:
            # Set our file
            os.rename(ntf.name, os.path.join(self.path, io_file))

        except (OSError, IOError):

            return False

    def open(self, key=None, mode="rb", encoding=None):
        """
        Returns an iterator to our our
        """

        if not self.asset.persistent_storage:
            return None

        if key is None:
            key = self.base_key

        elif not isinstance(key, str) or not self.__valid_token.match(key):
            raise AttributeError(
                f"Persistent Storage key ({key} provided is invalid")

        if encoding is None:
            encoding = self.encoding

        if key is None:
            key = self.base_key

        io_file = os.path.join(self.__base_path, f"{key}.dat")
        return open(io_file, mode=mode, encoding=encoding)

    def get(self, key, default=None, lazy=True):
        """
        Fetches from cache
        """
        if not self.asset.persistent_storage:
            return default

        if self._cache is None and not self.__load_cache():
            return default

        return self._cache[key].value \
            if key in self._cache and self._cache[key] else default

    def set(self, key, value, expires=None, lazy=True):
        """
        Cache reference
        """

        if not self.asset.persistent_storage:
            return False

        if self._cache is None and not self.__load_cache():
            return False

        # Fetch our cache value
        if lazy:
            try:
                prev = self._cache[key].value
                if prev == value and self._cache[key].expiry == expires:
                    # We're done
                    return True

            except KeyError:
                pass

        # Store our new cache
        self._cache[key] = CacheObject(value, expires)

        # Set our dirty flag
        self.__dirty = True

        if self.method == PersistentStoreMode.FORCE:
            # Flush changes to disk
            return self.flush()

        return True

    def prune(self):
        """
        Eliminates expired cache entries
        """
        change = False
        for key in list(self._cache.keys()):
            if not self._cache:
                del self._cache[key]
                if not change:
                    change = True

        return change

    def __load_cache(self):
        """
        Loads our cache
        """
        # Prepare our cache file
        cache_file = os.path.join(self.__base_path, self.cache_file)
        self.__dirty = False
        try:
            with gzip.open(cache_file, 'rb') as f:
                # Read our content from disk
                self._cache = {}
                for k, v in json.loads(f.read().decode(self.encoding)).items():
                    co = CacheObject.instantiate(v)
                    if co:
                        # Verify our object before assigning it
                        self._cache[k] = co

                    elif not self.__dirty:
                        # Track changes from our loadset
                        self.__dirty = True

        except (UnicodeDecodeError, json.decoder.JSONDecodeError):
            # Let users known there was a problem
            self._cache = {}
            logger.warning(
                'Corrupted access persistent cache content'
                f' {cache_file}')

        except FileNotFoundError:
            # No problem; no cache to load
            self._cache = {}

        except OSError as e:
            # We failed (likely a permission issue)
            logger.warning(
                'Could not load persistent cache for namespace %s',
                os.path.basename(self.__base_path))
            logger.debug('Persistent Storage Exception: %s' % str(e))
            return False

        # Ensure our dirty flag is set to False
        return True

    def flush(self, force=False):
        """
        Save's our cache
        """

        if not force and self.__dirty is False:
            # Nothing further to do
            logger.trace('Persistent cache consistent with memory map')
            return True

        # Ensure our path exists
        try:
            os.makedirs(self.__base_path, mode=0o770, exist_ok=True)

        except OSError:
            # Permission error
            logger.error('Could not create persistent store directory {}'
                         .format(self.__base_path))
            return False

        # Ensure our path exists
        try:
            os.makedirs(self.__temp_path, mode=0o770, exist_ok=True)

        except OSError:
            # Permission error
            logger.error('Could not create persistent store directory {}'
                         .format(self.__temp_path))
            return False

        # Unset our size lazy setting
        self.__size = None

        # Prepare our cache file
        cache_file = os.path.join(self.__base_path, self.cache_file)
        cache_file_backup = os.path.join(
            self.__base_path, self.cache_file_backup)

        if not self._cache:
            # We're deleting the file only
            try:
                os.unlink(f'{cache_file_backup}')

            except OSError:
                # No worries at all
                pass

            try:
                os.rename(f'{cache_file}', f'{cache_file_backup}')

            except OSError:
                # No worries at all
                pass
            return True

        # We're not deleting

        # ntf = NamedTemporaryFile
        ntf = None
        try:
            ntf = tempfile.NamedTemporaryFile(
                mode="w+", encoding=self.encoding, dir=self.__temp_path,
                delete=False)

            ntf.close()

        except OSError as e:
            logger.error(
                'Persistent temporary directory inaccessible: ',
                self.__temp_path)
            logger.debug('Persistent Storage Exception: %s' % str(e))

            if ntf:
                # Cleanup
                try:
                    ntf.close()
                except OSError:
                    pass

                try:
                    os.unlink(ntf.name)
                    logger.trace(
                        'Persistent temporary file removed: ', ntf.name)

                except OSError as e:
                    logger.error(
                        'Persistent temporary file removal failed: ', ntf.name)
                    logger.debug(
                        'Persistent Storage Exception: %s' % str(e))

            # Early Exit
            return False

        # update our content currently saved to disk
        with gzip.open(ntf.name, 'wb') as f:
            # Write our content to disk
            f.write(json.dumps(
                self._cache, separators=(',', ':'),
                cls=CacheJSONEncoder).encode(self.encoding))

        try:
            os.unlink(cache_file_backup)
            logger.trace(
                'Persistent cache backup file removed: ', cache_file_backup)

        except OSError:
            # No worries at all
            pass

        try:
            os.rename(cache_file, cache_file_backup)
            logger.trace(
                'Persistent cache file backed up: ', cache_file_backup)

        except OSError as e:
            if os.path.isfile(cache_file):
                logger.warning(
                    'Persistent cache file backup failed: ', ntf.name)
                logger.debug('Persistent Storage Exception: %s' % str(e))

                # Clean-up if posible
                try:
                    os.unlink(ntf.name)
                    logger.trace(
                        'Persistent temporary file removed: ', ntf.name)

                except OSError:
                    pass

                return False

        try:
            # Rename our file over the original
            os.rename(ntf.name, cache_file)
            logger.trace(
                'Persistent temporary file installed successfully: ',
                cache_file)

        except OSError:
            # This isn't good... we couldn't put our new file in place
            logger.error(
                'Could not write persistent content to ', cache_file)

            # Clean-up if posible
            try:
                os.unlink(ntf.name)
                logger.trace(
                    'Persistent temporary file removed: ', ntf.name)

            except OSError:
                pass

            return False

        logger.trace('Persistent cache generated for ', cache_file)
        # Ensure our dirty flag is set to False
        self.__dirty = False
        return True

    def size(self, lazy=True):
        """
        Returns the total size of the persistent storage in bytes
        """

        if not self.asset.persistent_storage:
            return 0

        if lazy and self.__size:
            return self.__size

        if self.__exclude_size_list is None:
            # A list of criteria that should be excluded from the size count
            self.__exclude_size_list = (
                # Exclude backup cache file from count
                re.compile(re.escape(os.path.join(
                    self.__base_path, self.cache_file_backup))),
                # Exclude temporary files
                re.compile(re.escape(self.__temp_path) + r'[/\\].+'),
            )

        # Get a list of files (file paths) in the given directory
        self.__size = 0

        try:
            self.__size += sum(
                [os.stat(path).st_size for path in filter(os.path.isfile,
                 glob.glob(self.__base_path + '/**/*', recursive=True))
                 if next((False for p in self.__exclude_size_list
                          if p.match(path)), True)])

        except (OSError, IOError):
            # We can't access the directory or it does not exist
            pass

        return self.__size

    def __del__(self):
        """
        Deconstruction of our object
        """

        if self.method == PersistentStoreMode.AUTO:
            # Flush changes to disk
            self.flush()

    def __delitem__(self, key):
        """
        Remove a cache entry by it's key
        """
        try:
            # Store our new cache
            del self._cache[key]
            # Set our dirty flag
            self.__dirty = True

        except KeyError:
            # Nothing to do
            raise

        if self.method == PersistentStoreMode.FORCE:
            # Flush changes to disk
            self.flush()

        return

    def __contains__(self, key):
        """
        Verify if our storage contains the key specified or not.
        In additiont to this, if the content is expired, it is considered
        to be not contained in the storage.
        """
        if self._cache is None and not self.__load_cache():
            return False

        return key in self._cache and self._cache[key]

    def __setitem__(self, key, value):
        """
        Sets a cache value
        """
        if not self.set(key, value):
            raise OSError("Could not set cache")

        return

    def __getitem__(self, key):
        """
        Returns the indexed value
        """
        NOT_FOUND = (None, None)
        result = self.get(key, default=NOT_FOUND, lazy=False)
        if result is NOT_FOUND:
            raise KeyError()

        return result

    @property
    def path(self):
        """
        Returns the full path to the namespace directory
        """
        return self.__base_path
