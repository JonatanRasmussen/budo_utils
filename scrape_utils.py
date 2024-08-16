import string
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse
from pathlib import Path
from typing import Optional, Union, Dict, List

class ScrapeUtils:
    """My personal scraping utils, copy-pasted from another project."""

    class Trimmer:
        """Text and HTML trimming rulesets for scraping purposes."""

        _trimmer_registry: Dict[str, 'ScrapeUtils.Trimmer'] = {}

        def __init__(self, target_url: str, start: str, end: str) -> None:
            """Initialize trimmer configuration."""
            self.target_url: str = target_url
            self.start: str = start
            self.end: str = end

        @staticmethod
        def register_trimming_ruleset(target_url: str, start: str, end: str) -> None:
            """Create and register a trimming ruleset to be used by urls matching target_url"""
            new_trimmer = ScrapeUtils.Trimmer(target_url, start, end)
            if target_url in ScrapeUtils.Trimmer._trimmer_registry:
                existing_trimmer = ScrapeUtils.Trimmer._trimmer_registry[target_url]
                if not ScrapeUtils.Trimmer._is_equal(new_trimmer, existing_trimmer):
                    print(f"Warning: Existing trimmer with target_url {target_url} was overwritten")
            ScrapeUtils.Trimmer._trimmer_registry[target_url] = new_trimmer

        @staticmethod
        def trim_html(url: str, html: str) -> str:
            """ Find the trimming ruleset that matches the url and apply it on the html """
            matches: List[ScrapeUtils.Trimmer] = []
            for key, value in ScrapeUtils.Trimmer._trimmer_registry.items():
                if key in url: # Check the registry for any trimmers with a matching target_url
                    matches.append(value)
            if len(matches) == 1: # If exactly one trimmer is found, use that
                trimmer = matches[0]
                return ScrapeUtils.Trimmer._trim_start_and_end(html, trimmer.start, trimmer.end)
            if len(matches) > 1: # If not exactly one trimmers is found, do no trimming
                print(f"Warning: Url {url} matched {len(matches)} trimmers. Trimming was skipped")
            return html

        @staticmethod
        def trim_start_and_end(text_to_trim: str, start: str, end: str) -> str:
            """Trim away everything before 'start' and everything after 'end'"""

            def _trim_start(text: str, start: str) -> str:
                """Trim away everything before 'start' """
                start_indices = [i for i in range(len(text)) if text.startswith(start, i)]
                if len(start_indices) == 0:
                    print("Error: 'start' string not found in the text.")
                    return text
                if len(start_indices) > 1:
                    print("Error: 'start' string found multiple times in the text.")
                    return text
                start_index = start_indices[0]
                return text[start_index:]

            def _trim_end(text: str, end: str) -> str:
                """Trim away everything after 'end' """
                end_indices = [i for i in range(len(text)) if text.startswith(end, i)]
                if len(end_indices) == 0:
                    print("Error: 'end' string not found in the text.")
                    return text
                if len(end_indices) > 1:
                    print("Error: 'end' string found multiple times in the text.")
                    return text
                end_index = end_indices[-1]
                return text[:end_index + len(end)]

            text_to_trim = _trim_start(text_to_trim, start)
            text_to_trim = _trim_end(text_to_trim, end)
            return text_to_trim

        @staticmethod
        def _trim_start_and_end(text_to_trim: str, start: str, end: str) -> str:
            """Trim away everything before 'start' and everything after 'end'"""
            start_index = text_to_trim.find(start)
            if start_index != -1 and len(start) != 0:
                text_to_trim = text_to_trim[start_index:]
            end_index = text_to_trim.rfind(end)
            if end_index != -1:
                text_to_trim = text_to_trim[:end_index + len(end)]
            return text_to_trim

        @staticmethod
        def _is_equal(trimmer1: 'ScrapeUtils.Trimmer', trimmer2: 'ScrapeUtils.Trimmer') -> bool:
            """Compares if two trimmers are equal in their target_url and ruleset"""
            equal_url = trimmer1.target_url == trimmer2.target_url
            equal_start = trimmer1.start == trimmer2.start
            equal_end = trimmer1.end == trimmer2.end
            return equal_url and equal_start and equal_end


    class Persistence:
        """Custom persistence and FileIO solution for scraping purposes."""
        # Paths and folders
        _workspace_path: Path = Path.cwd()

        @staticmethod
        def read_textfile(path: Union[Path, str], missing_ok: bool = False) -> str:
            """Read a text file, optionally allowing for missing files."""
            path = ScrapeUtils.Persistence._resolve_path(path)
            try:
                with open(path, 'r', encoding='utf-8') as file:
                    return file.read()
            except FileNotFoundError:
                if not missing_ok:
                    print(f"Error: File not found: {path}")
                    raise
            except PermissionError:
                print(f"Error: Permission denied: {path}")
                raise
            except IOError as e:
                print(f"Error: IO error occurred while reading {path}: {e}")
                raise
            return ""

        @staticmethod
        def write_textfile(path: Union[Path, str], content: str) -> None:
            """Write content to a text file."""
            path = ScrapeUtils.Persistence._resolve_path(path)
            try:
                with open(path, 'w', encoding='utf-8') as file:
                    if content:
                        file.write(content)
                    else:
                        print(f"Warning: Empty file created at {path}")
            except PermissionError:
                print(f"Error: Permission denied when writing to {path}")
                raise
            except IOError as e:
                print(f"Error: IO error occurred while writing to {path}: {e}")
                raise

        @staticmethod
        def _resolve_path(path: Union[Path, str]) -> Path:
            """Attempt to make a relative or missing path absolute."""
            if isinstance(path, str):
                path = Path(path)
            if not path.is_absolute():
                path = ScrapeUtils.Persistence._workspace_path / path
            directory = path.parent
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
            resolved_path = path.resolve()
            return resolved_path

    class Html:
        """HTML handling and caching utility for scraping purposes."""

        html_webcache_folder: Path = Path.cwd() / "webcache"
        feature_flag_read_webcache: bool = True
        feature_flag_write_webcache: bool = True

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
                        print(f"Warning: Path for url {url} not found in paths dictionary")
                html = ScrapeUtils.Html.fetch_url(url, path, timeout)
                html_dct[url] = html
            return html_dct

        @staticmethod
        def fetch_url(url: str, path: Optional[Path] = None, timeout: Union[int,float] = 10) -> str:
            """Scrape HTML content from a given URL. Uses cached content if available."""
            cached_html = ScrapeUtils.Html.try_get_cached_html(url, path)
            if cached_html:
                return cached_html
            html = ScrapeUtils.Html._send_request(url, timeout=timeout)
            ScrapeUtils.Html.cache_html_for_later(url, html, path)
            return html

        @staticmethod
        def cache_html_for_later(url: str, html: str, path: Optional[Path] = None) -> None:
            """Cache HTML content in memory and optionally on disk."""
            trimmed_html = ScrapeUtils.Trimmer.trim_html(url, html)
            ScrapeUtils.Html._webcache[url] = trimmed_html
            if ScrapeUtils.Html.feature_flag_write_webcache:
                ScrapeUtils.Html._write_html_to_disk(url, trimmed_html, path=path)

        @staticmethod
        def try_get_cached_html(url: str, path: Optional[Path] = None) -> str:
            """Attempt to retrieve cached HTML from in-memory cache or disk cache."""
            if url in ScrapeUtils.Html._webcache:
                return ScrapeUtils.Html._webcache[url]
            cached_html = ScrapeUtils.Html._search_url_in_local_webcache(url, path=path)
            if cached_html:
                ScrapeUtils.Html._webcache[url] = cached_html
                return cached_html
            return ""

        @staticmethod
        def _search_url_in_local_webcache(url: str, path: Optional[Path] = None) -> str:
            """Search for cached HTML content on disk for a given URL."""
            if ScrapeUtils.Html.feature_flag_read_webcache:
                if path is None:
                    path = ScrapeUtils.Html._get_path_for_cached_html(url)
                html = ScrapeUtils.Persistence.read_textfile(path, missing_ok=True)
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
                print(f"Error: A url or http error occurred: {e}")
                return ""

        @staticmethod
        def _write_html_to_disk(url: str, html: str, path: Optional[Path] = None) -> None:
            """Write HTML content to disk cache."""
            if path is None:
                path = ScrapeUtils.Html._get_path_for_cached_html(url)
            ScrapeUtils.Persistence.write_textfile(path, html)

        @staticmethod
        def _get_path_for_cached_html(url: str) -> Path:
            """Generate a file path for caching HTML content based on the URL."""
            file_ext = ScrapeUtils.Html._default_webcache_file_ext
            if '.' not in file_ext:
                file_ext = f".{file_ext}"
            file = f"{ScrapeUtils.Html._generate_filename(url)}{file_ext}"
            folder = ScrapeUtils.Html._generate_foldername(url)
            return ScrapeUtils.Html.html_webcache_folder / folder / file

        @staticmethod
        def _generate_foldername(url: str) -> str:
            """
            Converts input: 'https://example.com/path?q=123' to the output: 'example'
            or 'https://www.test.com' to 'test',
            or 'https://mail.admin.dtu.dk' to 'mail_admin_dtu'
            """
            domain = urlparse(url).netloc.split(':')[0] # Remove port if present
            domain_parts = domain.split('.')
            domain_parts = domain_parts[:-1] # Remove toplevel domain ('.com', '.dk', '.net' etc.)
            if len(domain_parts) > 1 and domain_parts[0].lower() == 'www':
                domain_parts = domain_parts[1:] # If url starts with 'www' then remove it
            return '_'.join(domain_parts) # Join the remaining parts with underscores

        @staticmethod
        def _generate_filename(url: str) -> str:
            """
            Generate a filename from a URL, removing invalid characters.
            Example: "https://example.com/path?query=123" becomes "example_com_path_query_123
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