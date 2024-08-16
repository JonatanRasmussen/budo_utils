import gzip
import io
import logging
import shutil
import string
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Union, Optional
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


class BudoConfig:
    """Custom configs, filepaths and constantes for Budo's personal projects."""
    # Top level folders:
    workspace_path: Path = Path.cwd() #Path("/path/to/your/workspace")
    budolib_topfolder: Path = workspace_path / "budolib"
    budolib_datafolder: Path = workspace_path / "data"

    # Compressed folder name
    compressed_folder_name: str = "compressed_backups"
    uncompressed_folder_name: str = "uncompressed_files"
    feature_flag_try_write_compressed_backups: bool = True
    feature_flag_try_read_compressed_backups: bool = True

    # BudoLogger:
    logging_top_folder: Path = budolib_topfolder / "logging"
    log_archive_folder: Path = logging_top_folder / "archive"
    log_history_folder: Path = logging_top_folder / "history"
    log_session_folder: Path = logging_top_folder / "session"
    log_history_enable_timeout: bool = False
    log_history_timeout_duration: int = 48

    # BudoWebscraper
    webscraper_default_timeout: float = 10

    # BudoHtml
    html_webcache_folder: Path = budolib_datafolder / "webcache"
    feature_flag_read_webcache: bool = True
    feature_flag_write_webcache: bool = True


class BudoLogger:
    """Custom logging solution for Budo's personal projects."""
    # Paths and folders
    _workspace_path: Path = BudoConfig.workspace_path
    _log_archive_folder: Path = BudoConfig.log_archive_folder # is never deleted
    _log_history_folder: Path = BudoConfig.log_history_folder # is deleted after __ hours
    _log_session_folder: Path = BudoConfig.log_session_folder # is deleted at each code execution

    # Delete log history folder after __ hours:
    _log_history_enable_expiration: bool = BudoConfig.log_history_enable_timeout
    _log_history_expiration_hour: int = BudoConfig.log_history_timeout_duration

    # Console print config
    _log_print_threshhold: int = logging.INFO

    # Multiple loggers are run in parallel, one for each folder/level combo
    _loggers: Dict[str, logging.Logger] = {}

    # Booleans to ensure setup happens once
    _setup_in_progress: bool = False
    _setup_done: bool = False


    @staticmethod
    def debug(message: str) -> None:
        """Log a debug message."""
        BudoLogger._try_print_and_setup(logging.DEBUG, message)
        BudoLogger._loggers['debug'].debug(message)

    @staticmethod
    def info(message: str) -> None:
        """Log an info message."""
        BudoLogger._try_print_and_setup(logging.INFO, message)
        BudoLogger._loggers['info'].info(message)

    @staticmethod
    def warning(message: str) -> None:
        """Log a warning message."""
        BudoLogger._try_print_and_setup(logging.WARNING, message)
        BudoLogger._loggers['warning'].warning(message)

    @staticmethod
    def error(message: str) -> None:
        """Log an error message."""
        BudoLogger._try_print_and_setup(logging.ERROR, message)
        BudoLogger._loggers['error'].error(message)

    @staticmethod
    def critical(message: str) -> None:
        """Log a critical message."""
        BudoLogger._try_print_and_setup(logging.CRITICAL, message)
        BudoLogger._loggers['critical'].critical(message)

    @staticmethod
    def _convert_level_to_classification(level: int) -> str:
        """Convert numeric level to classification.
        For example, 40 (logging.ERROR) to 'Error'. """
        if level == logging.DEBUG:
            return "Debug"
        if level == logging.INFO:
            return "Info"
        if level == logging.WARNING:
            return "Warning"
        if level == logging.ERROR:
            return "Error"
        if level == logging.CRITICAL:
            return "Critical"
        return "Unknown"

    @staticmethod
    def _try_print_and_setup(level: int, message: str) -> None:
        """Shared logic that needs to be run for new log messages regardless of level"""
        BudoLogger._try_console_print_message(level, message)
        BudoLogger._try_run_setup()

    @staticmethod
    def _try_console_print_message(level: int, message: str) -> None:
        """Console print the log message if it meets the configured print threshhold"""
        if level >= BudoLogger._log_print_threshhold:
            classification = BudoLogger._convert_level_to_classification(level)
            print(f"Budo{classification}: {message}")

    @staticmethod
    def _try_run_setup() -> None:
        """Perform logger setup if not already done. Raise an error if setup is in progress."""
        if not BudoLogger._setup_done:
            if BudoLogger._setup_in_progress:
                raise BudoLogger.BudoLoggerAttemptedLoggingDuringSetupError
            BudoLogger._setup_in_progress = True
            BudoLogger._run_setup()
            BudoLogger._setup_in_progress = False
            BudoLogger._setup_done = True

    @staticmethod
    def _run_setup() -> None:
        """Run setup. Outer method _try_run_setup() handles duplicate setup issues"""
        BudoLogger._prepare_directories()
        levels = [
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ]
        folders = [
            BudoLogger._log_archive_folder,
            BudoLogger._log_history_folder,
            BudoLogger._log_session_folder,
        ]
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        for level in levels:
            level_name = logging.getLevelName(level).lower()
            logger = logging.getLogger(f'{level_name}')
            logger.setLevel(level)
            for folder in folders:
                if BudoLogger._severity_level_meets_folder_threshhold(level, folder):
                    handler = logging.FileHandler(folder / f"{level_name}.log")
                    handler.setLevel(level)
                    handler.setFormatter(formatter)
                    logger.addHandler(handler)
                    BudoLogger._loggers[level_name] = logger

    @staticmethod
    def _prepare_directories() -> None:
        """Create the necessary directories if they don't exist and clear the session logs."""
        session_folder = BudoLogger._log_session_folder
        history_folder = BudoLogger._log_history_folder
        archive_folder = BudoLogger._log_archive_folder

        # Handle session folder (delete at start of each code execution)
        if session_folder.exists():
            BudoLogger._delete_folder(session_folder)
        session_folder.mkdir(parents=True, exist_ok=True)

        # Handle the history folder (is deleted if log content is __ hours old)
        if BudoLogger._log_history_enable_expiration:
            expiration_hour = BudoLogger._log_history_expiration_hour
            if history_folder.exists():
                current_time = datetime.now(timezone.utc)
                for log_file in history_folder.glob('*.log'):
                    file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc)
                    if current_time - file_mtime > timedelta(hours=expiration_hour):
                        BudoLogger._delete_folder(history_folder)
                        break
            history_folder.mkdir(parents=True, exist_ok=True)

        # Handle archive folder (is never deleted)
        archive_folder.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _delete_folder(path: Path) -> None:
        """Delete folder at the specified path, unless it is outside the current workspace"""
        if path.is_relative_to(BudoLogger._workspace_path):
            shutil.rmtree(path) # For safety, ensure path is inside workspace

    @staticmethod
    def _severity_level_meets_folder_threshhold(level: int, folder: Path) -> bool:
        if folder == BudoLogger._log_history_folder:
            severity_threshold = logging.INFO
        elif folder == BudoLogger._log_archive_folder:
            severity_threshold = logging.WARNING
        else:
            severity_threshold = logging.NOTSET
        return level >= severity_threshold

    class BudoLoggerAttemptedLoggingDuringSetupError(Exception):
        """Custom exception for logging setup issues."""
        def __init__(self):
            super().__init__("Error: BudoLogger logged something to itself during its setup process")


class BudoPersistence:
    """Custom persistence and FileIO solution for Budo's personal projects."""
    # Paths and folders
    _workspace_path = BudoConfig.workspace_path
    _in_recursion_loop: bool = False #for when read_textfile calls itself after file decompression

    @staticmethod
    def try_read_textfile(path: Union[Path, str]) -> str:
        """Attempt to read a text file, returning an empty string if not found."""
        file_missing_is_ok = True
        return BudoPersistence.read_textfile(path, missing_ok=file_missing_is_ok)

    @staticmethod
    def read_textfile(path: Union[Path, str], missing_ok: bool = False) -> str:
        """Read a text file, optionally allowing for missing files."""
        path = BudoPersistence._resolve_path(path)
        try:
            with open(path, 'r', encoding='utf-8') as file:
                return file.read()
        except FileNotFoundError:
            if BudoConfig.feature_flag_try_read_compressed_backups:
                if BudoPersistence.try_decompress_file(path):
                    return BudoPersistence._try_recursive_call(path, missing_ok=missing_ok)
            if not missing_ok:
                BudoLogger.error(f"File not found: {path}")
                raise
        except PermissionError:
            BudoLogger.error(f"Permission denied: {path}")
            raise
        except IOError as e:
            BudoLogger.error(f"IO error occurred while reading {path}: {e}")
            raise
        return ""

    @staticmethod
    def write_textfile(path: Union[Path, str], content: str) -> None:
        """Write content to a text file."""
        path = BudoPersistence._resolve_path(path)
        try:
            with open(path, 'w', encoding='utf-8') as file:
                if content:
                    file.write(content)
                else:
                    BudoLogger.warning(f"Empty file created at {path}")
            if BudoConfig.feature_flag_try_write_compressed_backups:
                BudoPersistence.try_compress_file(path)
        except PermissionError:
            BudoLogger.error(f"Permission denied when writing to {path}")
            raise
        except IOError as e:
            BudoLogger.error(f"IO error occurred while writing to {path}: {e}")
            raise

    @staticmethod
    def try_compress_file(path: Union[Path, str]) -> bool:
        """Attempt to compress a file, returning True if successful."""
        compressed_path = BudoPersistence._generate_compressed_subpath(path)
        uncompressed_path = BudoPersistence._generate_uncompressed_subpath(path)
        try:
            with open(uncompressed_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return True
        except FileNotFoundError:
            BudoLogger.warning(f"File not found for compression: {uncompressed_path}")
            return False
        except PermissionError:
            BudoLogger.error(f"Permission denied when compressing {path}")
            return False
        except IOError as e:
            BudoLogger.error(f"IO error occurred while compressing {path}: {e}")
            return False

    @staticmethod
    def try_decompress_file(path: Union[Path, str]) -> bool:
        """Attempt to decompress a file, returning True if successful."""
        compressed_path = BudoPersistence._generate_compressed_subpath(path)
        uncompressed_path = BudoPersistence._generate_uncompressed_subpath(path)
        try:
            with gzip.open(compressed_path, 'rb') as f_in:
                with open(uncompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            return True
        except FileNotFoundError:
            BudoLogger.warning(f"Compressed file not found for decompression: {compressed_path}")
            return False
        except PermissionError:
            BudoLogger.error(f"Permission denied when decompressing {path}")
            return False
        except IOError as e:
            BudoLogger.error(f"IO error occurred while decompressing {path}: {e}")
            return False

    @staticmethod
    def try_read_pandas_df(path: Union[Path, str]) -> pd.DataFrame:
        """Attempt to read a pandas df from a file, returning an empty DataFrame if not found."""
        return BudoPersistence.read_pandas_df(path, missing_ok=True)

    @staticmethod
    def read_pandas_df(path: Union[Path, str], missing_ok: bool = False) -> pd.DataFrame:
        """Read a pandas df from a file, optionally allowing for missing files."""
        try:
            content = BudoPersistence.read_textfile(path, missing_ok=missing_ok)
            if not content:
                return pd.DataFrame()
            df = pd.read_csv(io.StringIO(content))
            BudoLogger.info(f"DataFrame successfully read from {path}")
            return df
        except pd.errors.EmptyDataError:
            BudoLogger.warning(f"Empty dataframe loaded from {path}")
            return pd.DataFrame()
        except pd.errors.ParserError as e:
            BudoLogger.error(f"Error parsing CSV data from {path}: {e}")
            raise

    @staticmethod
    def write_pandas_df(path: Union[Path, str], df: pd.DataFrame, write_index: bool = False) -> None:
        """Write a pandas DataFrame to a file."""
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=write_index)
            csv_string = csv_buffer.getvalue()

            BudoPersistence.write_textfile(path, csv_string)
            BudoLogger.info(f"DataFrame successfully written to {path}")
        except IOError as e:
            BudoLogger.error(f"IO error occurred while writing DataFrame to {path}: {e}")
            raise

    @staticmethod
    def _try_recursive_call(path: Union[Path, str], missing_ok: bool = False) -> str:
        text = ""
        if not BudoPersistence._in_recursion_loop:
            BudoPersistence._in_recursion_loop = True
            text = BudoPersistence.read_textfile(path, missing_ok=missing_ok)
            BudoPersistence._in_recursion_loop = False
        BudoPersistence._in_recursion_loop = False
        return text

    @staticmethod
    def _generate_compressed_subpath(path: Union[Path, str]) -> Path:
        """Append subfolder named 'compressed' to end of path."""
        compressed_subfolder = BudoConfig.compressed_folder_name
        return BudoPersistence._add_subfolder_to_end_of_path(path, compressed_subfolder)

    @staticmethod
    def _generate_uncompressed_subpath(path: Union[Path, str]) -> Path:
        """Append subfolder named 'uncompressed' to end of path."""
        uncompressed_subfolder = BudoConfig.uncompressed_folder_name
        return BudoPersistence._add_subfolder_to_end_of_path(path, uncompressed_subfolder)

    @staticmethod
    def _add_subfolder_to_end_of_path(path: Union[Path, str], subfolder: str) -> Path:
        """Append subfolder to end of path."""
        path = BudoPersistence._resolve_path(path)
        if path.is_file():
            return path.parent / subfolder / path.name
        return path / subfolder

    @staticmethod
    def _resolve_path(path: Union[Path, str]) -> Path:
        """Attempt to make a relative or missing path absolute."""
        if isinstance(path, str):
            path = Path(path)
        if not path.is_absolute():
            path = BudoPersistence._workspace_path / path
        directory = path.parent
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
        resolved_path = path.resolve()
        return resolved_path


class BudoWebscraper:
    """Web scraping utility for Budo's personal projects."""

    # Feature flags
    feature_flag_read_webcache: bool = BudoConfig.feature_flag_read_webcache
    feature_flag_write_webcache: bool = BudoConfig.feature_flag_write_webcache

    # Static driver:
    _static_driver: Optional[WebDriver] = None

    # Default values:
    _default_driver: Optional[WebDriver] = None
    _default_url: str = ""
    _default_timeout: float = BudoConfig.webscraper_default_timeout
    _default_use_selenium: bool = True
    _default_force_webdriver_restart: bool = False
    _default_target_element: Union[str, List[str]] = ""

    def __init__(self, driver: Optional[WebDriver] = None) -> None:
        """Initialize the web scraper with an optional WebDriver."""
        self.external_driver: Optional[WebDriver] = driver
        self.most_recent_url: str = BudoWebscraper._default_url
        self.most_recent_webdriver_url: str = BudoWebscraper._default_url
        self.timeout: Union[int,float] = BudoWebscraper._default_timeout
        self.use_selenium: bool = BudoWebscraper._default_use_selenium
        self.force_webdriver_restart: bool = BudoWebscraper._default_force_webdriver_restart
        self.target_element: Union[str, List[str]] = BudoWebscraper._default_target_element

    @staticmethod
    def create_webdriver(headless: bool = False, proxy: Optional[str] = None) -> WebDriver:
        """Create a new WebDriver instance with optional headless mode and proxy."""
        options = Options()
        options.add_argument('--disable-logging')
        options.add_argument("--log-level=3")
        if headless:
            options.add_argument("--headless")
        if proxy:
            options.add_argument(f'--proxy-server={proxy}')
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        return driver

    @staticmethod
    def scrape_urls(urls: List[str],
                    paths: Optional[Dict[str,Path]] = None,
                    driver: Optional[WebDriver] = None,
                    timeout: Union[int,float] = 10,
                    use_selenium: bool = False,
                    target_element: Union[str, List[str]] = "") -> Dict[str, str]:
        budo_webscraper = BudoWebscraper(driver=driver)
        budo_webscraper.timeout = timeout
        budo_webscraper.use_selenium = use_selenium
        budo_webscraper.force_webdriver_restart = True
        budo_webscraper.target_element = target_element
        html_dct: Dict[str, str] = {}
        for url in urls:
            path = None
            if paths is not None:
                if url in paths:
                    path = paths[url]
                else:
                    BudoLogger.warning(f"Path for url {url} not found in paths dictionary")
            html = budo_webscraper.scrape_url(url, path=path, driver=driver)
            html_dct[url] = html
        budo_webscraper.quit_static_webdriver()
        return html_dct

    def scrape_url(self, url: str, path: Optional[Path] = None, driver: Optional[WebDriver] = None) -> str:
        """Scrape HTML content from a given URL. Uses cached content if available."""
        self.most_recent_url = url
        # If use_selenium is false, no driver is needed.
        if not self.use_selenium:
            return BudoHtml.fetch_url(url, timeout=self.timeout)
        # Look if cached_html is available before resorting to webdriving
        cached_html = BudoHtml.try_get_cached_html(self.most_recent_url, path=path)
        if cached_html:
            return cached_html
        # Spin up a webdriver or re-use an existing one
        self.most_recent_webdriver_url = self.most_recent_url
        driver_to_use = self._decide_which_driver_to_use(driver)
        html = self._access_url_via_webdriver(driver_to_use)
        BudoHtml.cache_html_for_later(self.most_recent_url, html, path)
        return html

    @staticmethod
    def quit_static_webdriver() -> None:
        """Quit the static WebDriver if it exists."""
        if BudoWebscraper._static_driver is not None:
            BudoWebscraper._static_driver.quit()
            BudoWebscraper._static_driver = BudoWebscraper._default_driver

    @staticmethod
    def _get_static_webdriver() -> WebDriver:
        """Get or create a static WebDriver instance."""
        if BudoWebscraper._static_driver is None:
            BudoWebscraper._static_driver = BudoWebscraper.create_webdriver()
        return BudoWebscraper._static_driver

    def _decide_which_driver_to_use(self, driver: Optional[WebDriver] = None) -> WebDriver:
        """
        Determine which WebDriver to use based on priority:
        1. Driver passed to scrape_html
        2. External driver
        3. Static class-level driver
        """
        if driver is not None: # 1st prio is any driver given to scrape_html as optional parameter
            return driver
        if self.external_driver is not None: # 2nd prio is any external drivers available
            return self.external_driver
        if self.force_webdriver_restart:
            BudoWebscraper.quit_static_webdriver()
        return BudoWebscraper._get_static_webdriver() # 3rd prio is the class-level static driver

    def _access_url_via_webdriver(self, driver: WebDriver) -> str:
        """Wait for page load, handle timeouts and return the page HTML for the url via driver."""
        driver.get("about:blank") # Clear the page source of the last url visited
        driver.get(self.most_recent_url)
        if self.target_element is not None:
            self._wait_for_target_element(driver)
        try:
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, 'html'))
            )
        except TimeoutException:
            BudoLogger.warning(f"Timeout after {self.timeout} seconds waiting for page to load")
            return ""
        html_element = driver.find_element(By.TAG_NAME, 'html')
        html = html_element.get_attribute('outerHTML')
        if html is None:
            BudoLogger.error("Get_attribute('innerHTML') returned None")
            return ""
        return html

    def _wait_for_target_element(self, driver: WebDriver) -> None:
        """Waits for one of the elements in target_element to be present on page"""
        if isinstance(self.target_element, str):
            target_elements = [self.target_element]
        else:
            target_elements = self.target_element
        ids_to_wait_for = ','.join(f'#{id}' for id in target_elements)
        try:
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ids_to_wait_for)),
            )
        except TimeoutException:
            BudoLogger.warning(f"WebDriver timeout after {self.timeout} seconds waiting for page to load (url = {self.most_recent_url})")


class BudoTrimmer:
    """Text and HTML trimming rulesets for Budo's personal projects."""

    _trimmer_registry: Dict[str, 'BudoTrimmer'] = {}

    def __init__(self, target_url: str, start: str, end: str,
                 replacements: Optional[Dict[str, str]] = None,
                 validations: Optional[List[str]] = None) -> None:
        """Initialize trimmer configuration."""
        self.target_url: str = target_url
        self.start: str = start
        self.end: str = end
        self.replacements: Optional[Dict[str, str]] = replacements
        self.validations: Optional[List[str]] = validations
        # Trimmers are not automatically added to registry, use register_trimmer() instead

    @staticmethod
    def register_trimming_ruleset(target_url: str, start: str, end: str,
                                  replacements: Optional[Dict[str, str]] = None,
                                  validations: Optional[List[str]] = None) -> None:
        """Create and register a trimmer ruleset to be used for urls matching target_url"""
        new_trimmer = BudoTrimmer(target_url, start, end, replacements, validations)
        if target_url in BudoTrimmer._trimmer_registry:
            existing_trimmer = BudoTrimmer._trimmer_registry[target_url]
            if not BudoTrimmer._trimmers_are_equal(new_trimmer, existing_trimmer):
                BudoLogger.warning(f"Existing trimmer with target_url {target_url} was overwritten")
        BudoTrimmer._trimmer_registry[target_url] = new_trimmer

    @staticmethod
    def trim_html(url: str, html: str, trimmer: Optional['BudoTrimmer'] = None) -> str:
        """ Find the trimming ruleset that matches the url and apply it on the html """
        if trimmer is not None: # If a trimmer was passed as function parameter, use that
            return trimmer.trim_and_validate(html)
        matches: List[BudoTrimmer] = []
        for key, value in BudoTrimmer._trimmer_registry.items():
            if key in url: # Check the registry for any trimmers with a matching target_url
                matches.append(value)
        if len(matches) == 1: # If exactly one trimmer is found, use that
            return matches[0].trim_and_validate(html)
        if len(matches) > 1: # If not exactly one trimmer is found, do no trimming
            BudoLogger.warning(f"Url {url} matched {len(matches)} trimmers. Trimming is skipped")
        return html

    def trim_and_validate(self, text_to_trim: str) -> str:
        """
        Performs the following actions on input, then returns it:
        1. Trim away everything before 'start'.
        2. Trim away everything after 'end'.
        3. For each key-value pair in 'replacements', look for key and replace with value.
        4. Search for each element in 'validations'. If not present, raise a warning.
        """
        trimmed_text = BudoTrimmer._trim_start_and_end(text_to_trim, self.start, self.end)
        replaced_text = BudoTrimmer._replace_substrings(trimmed_text, self.replacements)
        validated_text = BudoTrimmer._validate_remaining_text(replaced_text, self.validations)
        return validated_text

    @staticmethod
    def _trim_start_and_end(text_to_trim: str, start: str, end: str) -> str:
        """Trim away everything before 'start' and everything after 'end'"""

        def _trim_start(text: str, start: str) -> str:
            """Trim away everything before 'start' """
            start_indices = [i for i in range(len(text)) if text.startswith(start, i)]
            if len(start_indices) == 0:
                BudoLogger.warning("'start' string not found in the text.")
                return text
            if len(start_indices) > 1:
                BudoLogger.warning("'start' string found multiple times in the text.")
                return text
            start_index = start_indices[0]
            return text[start_index:]

        def _trim_end(text: str, end: str) -> str:
            """Trim away everything after 'end' """
            end_indices = [i for i in range(len(text)) if text.startswith(end, i)]
            if len(end_indices) == 0:
                BudoLogger.warning("'end' string not found in the text.")
                return text
            if len(end_indices) > 1:
                BudoLogger.warning("'end' string found multiple times in the text.")
                return text
            end_index = end_indices[-1]
            return text[:end_index + len(end)]

        text_to_trim = _trim_start(text_to_trim, start)
        text_to_trim = _trim_end(text_to_trim, end)
        return text_to_trim

    @staticmethod
    def _replace_substrings(text_to_trim: str, replacements: Optional[Dict[str, str]]) -> str:
        """For each key-value pair in 'replacements', look for key and replace with value."""
        if replacements is not None:
            for old, new in replacements.items():
                text_to_trim = text_to_trim.replace(old, new)
        return text_to_trim

    @staticmethod
    def _validate_remaining_text(text_to_trim: str, validations: Optional[List[str]]) -> str:
        """Search for each element in 'validations'. If not present, raise a warning."""
        if validations is not None:
            for validation in validations:
                if validation not in text_to_trim:
                    BudoLogger.warning(f"Validation string '{validation}' not found in the text")
        return text_to_trim

    @staticmethod
    def _trimmers_are_equal(trimmer1: 'BudoTrimmer', trimmer2: 'BudoTrimmer') -> bool:
        """Compares if two trimmers are equal in their target_url and ruleset"""
        equal_url = trimmer1.target_url == trimmer2.target_url
        equal_start = trimmer1.start == trimmer2.start
        equal_end = trimmer1.end == trimmer2.end
        equal_replace = trimmer1.replacements == trimmer2.replacements
        equal_validate = trimmer1.validations == trimmer2.validations
        return equal_url and equal_start and equal_end and equal_replace and equal_validate


class BudoHtml:
    """HTML handling and caching utility for Budo's personal projects."""

    # Paths and folders
    default_write_path: Path = BudoConfig.html_webcache_folder
    feature_flag_read_webcache: bool = BudoConfig.feature_flag_read_webcache
    feature_flag_write_webcache: bool = BudoConfig.feature_flag_write_webcache

    # Default values:
    _default_webcache_file_ext: str = ".txt"

    # In-memory cache
    _webcache: Dict[str, str] = {}

    @staticmethod
    def fetch_urls(urls: List[str],
                   paths: Optional[Dict[str,Path]] = None,
                   timeout: Union[int,float] = 10) -> Dict[str, str]:
        """Call fetch_url for multiple urls"""
        html_dct: Dict[str, str] = {}
        for url in urls:
            path = None
            if paths is not None:
                if url in paths:
                    path = paths[url]
                else:
                    BudoLogger.warning(f"Path for url {url} not found in paths dictionary")
            html = BudoHtml.fetch_url(url, path, timeout)
            html_dct[url] = html
        return html_dct

    @staticmethod
    def fetch_url(url: str, path: Optional[Path] = None, timeout: Union[int,float] = 10) -> str:
        """Scrape HTML content from a given URL. Uses cached content if available."""
        cached_html = BudoHtml.try_get_cached_html(url, path)
        if cached_html:
            return cached_html
        html = BudoHtml._send_request(url, timeout=timeout)
        BudoHtml.cache_html_for_later(url, html, path)
        return html

    @staticmethod
    def cache_html_for_later(url: str, html: str, path: Optional[Path] = None) -> None:
        """Cache HTML content in memory and optionally on disk."""
        trimmed_html = BudoTrimmer.trim_html(url, html)
        BudoHtml._webcache[url] = trimmed_html
        if BudoHtml.feature_flag_write_webcache:
            BudoHtml._write_html_to_disk(url, trimmed_html, path=path)

    @staticmethod
    def try_get_cached_html(url: str, path: Optional[Path] = None) -> str:
        """Attempt to retrieve cached HTML from in-memory cache or disk cache."""
        if url in BudoHtml._webcache:
            return BudoHtml._webcache[url]
        cached_html = BudoHtml._search_url_in_local_webcache(url, path=path)
        if cached_html:
            BudoHtml._webcache[url] = cached_html
            return cached_html
        return ""

    @staticmethod
    def _search_url_in_local_webcache(url: str, path: Optional[Path] = None) -> str:
        """Search for cached HTML content on disk for a given URL."""
        if BudoHtml.feature_flag_read_webcache:
            if path is None:
                path = BudoHtml._get_path_for_cached_html(url)
            html = BudoPersistence.try_read_textfile(path)
            if html is not None:
                return html
        return ""

    @staticmethod
    def _send_request(url: str, timeout: Union[int,float] = 10) -> str:
        """Send an HTTP GET request to the specified URL and return the response text."""
        try:
            with urlopen(url, timeout=timeout) as response:
                return response.read().decode('utf-8')
        except (URLError, HTTPError) as e:
            BudoLogger.error(f"A url or http error occurred: {e}")
            return ""

    @staticmethod
    def _write_html_to_disk(url: str, html: str, path: Optional[Path] = None) -> None:
        """Write HTML content to disk cache."""
        if path is None:
            path = BudoHtml._get_path_for_cached_html(url)
        BudoPersistence.write_textfile(path, html)

    @staticmethod
    def _get_path_for_cached_html(url: str) -> Path:
        """Generate a file path for caching HTML content based on the URL."""
        file_ext = BudoHtml._default_webcache_file_ext
        if '.' not in file_ext:
            file_ext = f".{file_ext}"
        file = f"{BudoHtml._generate_filename(url)}{file_ext}"
        folder = BudoHtml._generate_foldername(url)
        return BudoHtml.default_write_path / folder / file

    @staticmethod
    def _generate_foldername(url: str) -> str:
        """
        Converts input: 'https://example.com/path?q=123' to the output: 'example'
        or 'https://www.test.com' to 'test',
        or 'https://mail.admin.dtu.dk' to 'mail_admin_dtu'
        """
        domain = urlparse(url).netloc.split(':')[0] # Remove port if present
        domain_parts = domain.split('.')
        domain_parts = domain_parts[:-1] # Remove top-level domain ('.com', '.dk', '.net' etc.)
        if len(domain_parts) > 1 and domain_parts[0].lower() == 'www':
            domain_parts = domain_parts[1:] # If url starts with 'www' then remove it
        return '_'.join(domain_parts) # Join the remaining parts with underscores

    @staticmethod
    def _generate_filename(url: str) -> str:
        """
        Generate a filename from a URL, extracting only the path and query after the domain.
        Example: "https://example.com/path?query=123" becomes "path_query_123"
        """
        # Parse the URL
        parsed_url = urlparse(url)

        # Get the path and query
        path_and_query = parsed_url.path + parsed_url.query

        # Remove the leading '/' if present
        if path_and_query.startswith('/'):
            path_and_query = path_and_query[1:]

        # Define the set of valid characters (alphanumeric and some punctuation)
        valid_chars = f"-_.{string.ascii_letters}{string.digits}"

        # Remove any characters not in the valid set
        sanitized_filename = ''.join(c if c in valid_chars else '_' for c in path_and_query)

        # Replace '/' with '_'
        sanitized_filename = sanitized_filename.replace('/', '_')

        # Remove leading and trailing underscores
        sanitized_filename = sanitized_filename.strip('_')

        # If the result is empty, use a default name
        if not sanitized_filename:
            sanitized_filename = ''.join(c if c in valid_chars else '_' for c in url)
        return sanitized_filename