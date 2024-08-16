import csv
import json
import re
from io import StringIO
from pathlib import Path
from typing import Optional, Set, Dict, Any, List

from scrape_utils import ScrapeUtils

class WowheadClassSpec:

    UNKNOWN_VALUE: str = "Unknown"

    def __init__(self, class_name: str, spec_dict: Dict[int, str]):
        """Initialize a WowheadClassSpec instance."""
        self.name: str = class_name
        self.spec_dict: Dict[int, str] = spec_dict

    @staticmethod
    def death_knight() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Death Knight."""
        return WowheadClassSpec("Death Knight", {
            250: "Blood",
            251: "Frost",
            252: "Unholy"
        })

    @staticmethod
    def demon_hunter() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Demon Hunter."""
        return WowheadClassSpec("Demon Hunter", {
            577: "Havoc",
            581: "Vengeance"
        })

    @staticmethod
    def druid() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Druid."""
        return WowheadClassSpec("Druid", {
            102: "Balance",
            103: "Feral",
            104: "Guardian",
            105: "Restoration"
        })

    @staticmethod
    def evoker() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Evoker."""
        return WowheadClassSpec("Evoker", {
            1467: "Devastation",
            1468: "Preservation",
            1473: "Augmentation"
        })

    @staticmethod
    def hunter() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Hunter."""
        return WowheadClassSpec("Hunter", {
            253: "Beast Mastery",
            254: "Marksmanship",
            255: "Survival"
        })

    @staticmethod
    def mage() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Mage."""
        return WowheadClassSpec("Mage", {
            62: "Arcane",
            63: "Fire",
            64: "Frost"
        })

    @staticmethod
    def monk() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Monk."""
        return WowheadClassSpec("Monk", {
            268: "Brewmaster",
            270: "Mistweaver",
            269: "Windwalker"
        })

    @staticmethod
    def paladin() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Paladin."""
        return WowheadClassSpec("Paladin", {
            65: "Holy",
            66: "Protection",
            70: "Retribution"
        })

    @staticmethod
    def priest() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Priest."""
        return WowheadClassSpec("Priest", {
            256: "Discipline",
            257: "Holy",
            258: "Shadow"
        })

    @staticmethod
    def rogue() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Rogue."""
        return WowheadClassSpec("Rogue", {
            259: "Assassination",
            260: "Outlaw",
            261: "Subtlety"
        })

    @staticmethod
    def shaman() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Shaman."""
        return WowheadClassSpec("Shaman", {
            262: "Elemental",
            263: "Enhancement",
            264: "Restoration"
        })

    @staticmethod
    def warlock() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Warlock."""
        return WowheadClassSpec("Warlock", {
            265: "Affliction",
            266: "Demonology",
            267: "Destruction"
        })

    @staticmethod
    def warrior() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Warrior."""
        return WowheadClassSpec("Warrior", {
            71: "Arms",
            72: "Fury",
            73: "Protection"
        })

    @staticmethod
    def unknown_class() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for an unknown class."""
        return WowheadClassSpec(WowheadClassSpec.UNKNOWN_VALUE, {})

    @staticmethod
    def get_class_name(spec_id: int) -> str:
        """Get the class name for a given spec ID."""
        wow_class = WowheadClassSpec.get_wow_class(spec_id)
        return wow_class.name

    @staticmethod
    def get_spec_name(spec_id: int) -> str:
        """Get the spec name for a given spec ID."""
        wow_class = WowheadClassSpec.get_wow_class(spec_id)
        return wow_class.spec_dict.get(spec_id, WowheadClassSpec.UNKNOWN_VALUE)

    @staticmethod
    def get_name(spec_id: int) -> str:
        """Get the full name (spec + class) for a given spec ID."""
        class_name = WowheadClassSpec.get_class_name(spec_id)
        spec_name = WowheadClassSpec.get_spec_name(spec_id)
        return f"{spec_name} {class_name}"

    @staticmethod
    def get_wow_class(spec_id: int) -> 'WowheadClassSpec':
        """Get the WowheadClassSpec instance for a given spec ID."""
        all_classes = WowheadClassSpec.get_all_classes()
        for wow_class in all_classes:
            if spec_id in wow_class.spec_dict:
                return wow_class
        return WowheadClassSpec.unknown_class()

    @staticmethod
    def get_other_class_specs(spec_id: int) -> List[int]:
        """Get a list of other spec IDs for the same class as the given spec ID."""
        wow_class = WowheadClassSpec.get_wow_class(spec_id)
        return list(wow_class.spec_dict.keys())

    @staticmethod
    def get_all_spec_dicts() -> Dict[int, str]:
        """Get a dictionary of all spec IDs and their corresponding spec names."""
        all_specs = {}
        for wow_class in WowheadClassSpec.get_all_classes():
            all_specs.update(wow_class.spec_dict)
        return all_specs

    @staticmethod
    def get_all_classes() -> List['WowheadClassSpec']:
        """Get a list of all WowheadClassSpec instances."""
        return [
            WowheadClassSpec.death_knight(), WowheadClassSpec.demon_hunter(),
            WowheadClassSpec.druid(), WowheadClassSpec.evoker(), WowheadClassSpec.hunter(),
            WowheadClassSpec.mage(), WowheadClassSpec.monk(), WowheadClassSpec.paladin(),
            WowheadClassSpec.priest(), WowheadClassSpec.rogue(), WowheadClassSpec.shaman(),
            WowheadClassSpec.warlock(), WowheadClassSpec.warrior()
        ]

class WowheadItem:
    """Represents a WoW item with data scraped from Wowhead."""

    csv_filename = "wowhead_items.csv"
    folder: Path = Path.cwd() / "wowhead_items"
    instances: Dict[int, 'WowheadItem'] = {}

    def __init__(self, item_id: int, html_string: str):
        """Initialize WowheadItem with item ID and HTML content."""
        self.item_id: int = item_id
        self.html_string: str = html_string
        self.parsed_data: Dict[str, Any] = {}
        self.parse(item_id)
        WowheadItem.instances[item_id] = self

    @staticmethod
    def export_all_items_to_csv() -> None:
        """Export all WowheadItem instances' parsed data to a CSV file """
        if not WowheadItem.instances:
            print("Warning: There are 0 Wowhead Items to export. No csv was created.")
            return

        all_keys: Set[str] = set() # Get all unique keys from all items
        for item in WowheadItem.instances.values():
            all_keys.update(item.parsed_data.keys())

        first_columns = ['item_id', 'dropped_by', 'gear_slot', 'gear_type', 'name']
        last_columns = ['spec_ids', 'spec_names']
        fieldnames: List[str] = [] # Create the fieldnames list with the desired order
        fieldnames.extend(first_columns.copy())
        middle_columns = sorted(list(all_keys - set(first_columns) - set(last_columns)))
        fieldnames.extend(middle_columns)
        fieldnames.extend(last_columns)

        csv_buffer = StringIO() # Use StringIO to create CSV content in memory
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()

        for item in WowheadItem.instances.values(): # Write data for each item
            # Ensure all fields are present, use empty string for missing fields
            row_data = {key: item.parsed_data.get(key, '') for key in fieldnames}
            for key, value in row_data.items():
                if isinstance(value, list): # Convert lists to strings for CSV compatibility
                    row_data[key] = ', '.join(map(str, value))
            writer.writerow(row_data)
        csv_content = csv_buffer.getvalue()

        csv_path = WowheadItem.csv_filename
        ScrapeUtils.Persistence.write_textfile(csv_path, csv_content)
        print(f"CSV file created successfully: {csv_path}")

    def parse(self, item_id: int) -> None:
        """Parse HTML content to extract item data."""
        self.parsed_data['item_id'] = item_id
        self.parsed_data['name'] = self.extract_content(r'<h1 class="heading-size-1">(.*?)</h1>')
        self.parsed_data['item_level'] = self.extract_content(r'Item Level <!--ilvl-->(\d+)')
        self.parsed_data['bind'] = "Soulbound" if "Binds when picked up" in self.html_string else "BoE"
        self.parsed_data['gear_slot'] = self.extract_content(r'<table width="100%"><tr><td>(.*?)</td>')
        self.parsed_data['gear_type'] = self.extract_item_subtype()
        self.parsed_data['unique'] = "Unique-Equipped" in self.html_string
        self.parsed_data['primary_stats'] = self.extract_primary_stats()
        self.parsed_data['secondary_stats'] = self.extract_secondary_stats()
        self.parsed_data['required_level'] = self.extract_content(r'Requires Level <!--rlvl-->(\d+)')
        self.parsed_data['sell_price'] = self.extract_sell_price()
        self.parsed_data['dropped_by'] = self.extract_content(r'Dropped by: (.*?)</div>')
        self.parsed_data['spec_ids'] = self.extract_spec_ids()
        self.parsed_data['spec_names'] = self.extract_spec_names()

    def extract_content(self, pattern: str) -> Optional[str]:
        """Extract content from HTML using regex pattern."""
        match = re.search(pattern, self.html_string)
        return match.group(1) if match else None

    def extract_item_subtype(self) -> Optional[str]:
        """Extract the item subtype (armor type or weapon type) from the HTML content."""
        pattern = r'<table width="100%"><tr><td>[^<]+</td><th><!--scstart\d+:\d+--><span class="q1">([^<]+)</span><!--scend--></th></tr></table>'
        match = re.search(pattern, self.html_string)
        return match.group(1) if match else None

    def extract_primary_stats(self) -> Dict[str, int]:
        stats = {}
        for stat in ['Agility', 'Strength', 'Intellect']:
            value = self.extract_content(rf'\+(\d+) {stat}')
            if value:
                stats[stat] = int(value)
        return stats

    def extract_secondary_stats(self) -> Dict[str, int]:
        stats = {}
        for stat in ['Critical Strike', 'Haste', 'Mastery', 'Versatility']:
            value = self.extract_content(rf'(\d+) {stat}')
            if value:
                stats[stat] = int(value)
        return stats

    def extract_sell_price(self) -> str:
        """Extract and format item sell price."""
        gold: Optional[str] = self.extract_content(r'<span class="moneygold">(\d+)</span>')
        silver: Optional[str] = self.extract_content(r'<span class="moneysilver">(\d+)</span>')
        copper: Optional[str] = self.extract_content(r'<span class="moneycopper">(\d+)</span>')
        return f"{gold or 0} gold, {silver or 0} silver, {copper or 0} copper"

    def get_parsed_data(self) -> Dict[str, Any]:
        """Return parsed item data."""
        return self.parsed_data

    def extract_spec_ids(self) -> List[int]:
        """Extract spec IDs from the HTML content."""
        spec_ids = []
        pattern = r'<div class="iconsmall spec(\d+)"'
        matches = re.findall(pattern, self.html_string)
        for match in matches:
            spec_ids.append(int(match))
        if len(spec_ids) == 0:
            all_specs = WowheadClassSpec.get_all_spec_dicts()
            return list(all_specs.keys())
        return spec_ids

    def extract_spec_names(self) -> List[str]:
        """Extract spec names from the parsed spec IDs"""
        spec_names: List[str] = []
        spec_ids = self.extract_spec_ids()
        for spec_id in spec_ids:
            name = WowheadClassSpec.get_name(spec_id)
            if name != WowheadClassSpec.UNKNOWN_VALUE:
                spec_names.append(name)
            else:
                print(f"Warning: Spec ID {spec_id} did not map to a name!")
        return spec_names

    def convert_to_json_and_save_to_disk(self) -> None:
        """Convert parsed data to JSON and save to disk."""
        json_str = json.dumps(self.parsed_data, indent=4)
        path = WowheadItem.folder / f"{self.item_id}.json"
        ScrapeUtils.Persistence.write_textfile(path, json_str)

    @staticmethod
    def scrape_wowhead_item(item_id: int) -> None:
        """Scrape item data from Wowhead and save it."""
        WowheadItem._set_trimmer_ruleset_for_wowhead_items()
        url = f"https://www.wowhead.com/item={item_id}"
        html_content = ScrapeUtils.Html.fetch_url(url)

        if len(html_content) != 0:
            wowhead_item: WowheadItem = WowheadItem(item_id, html_content)
            wowhead_item.convert_to_json_and_save_to_disk()

    @staticmethod
    def _set_trimmer_ruleset_for_wowhead_items() -> None:
        """In ScrapeUtils.Trimmer, register trimming ruleset for wowhead.com/item"""
        target_url = "wowhead.com/item="
        html_start = '<h1 class="heading-size-1">'
        html_end = '<h2 class="heading-size-2 clear">Related</h2></div>'
        ScrapeUtils.Trimmer.register_trimming_ruleset(target_url, html_start, html_end)

class WowheadZone:
    """Represents a WoW zone with data scraped from Wowhead."""

    folder: Path = Path.cwd() / "wowhead_zones"
    instances: Dict[int, 'WowheadZone'] = {}

    def __init__(self, zone_id: int, html_string: str):
        """Initialize WowheadItem with item ID and HTML content."""
        self.zone_id: int = zone_id
        self.html_string: str = html_string
        self.parsed_data: Dict[str, Any] = {}
        self.parse(zone_id)
        WowheadZone.instances[zone_id] = self

    @classmethod
    def get_all_item_ids(cls) -> List[int]:
        """
        Goes over each instance in the class variable 'instances' and returns a list of
        each item_id found in parsed_data['item_ids']
        """
        all_item_ids = []
        for instance in cls.instances.values():
            all_item_ids.extend(instance.parsed_data['item_ids'])
        return list(set(all_item_ids))  # Remove duplicates and return as list

    def parse(self, zone_id: int) -> None:
        """Parse HTML content to extract item IDs."""
        self.parsed_data['zone_id'] = zone_id
        self.parsed_data['name'] = self.extract_name()
        self.parsed_data['bosses'] = self.extract_bosses()
        self.parsed_data['boss_order'] = list(self.extract_bosses().values())
        self.parsed_data['item_ids'] = self.extract_item_ids()

    def extract_name(self) -> str:
        pattern = r'var myMapper = new Mapper\({"parent":"[^"]+","zone":\d+,"name":"([^"]+)"\}\);'
        match = re.search(pattern, self.html_string)
        return match.group(1) if match else "Unknown"

    def extract_bosses(self) -> Dict[int, str]:
        """Parse HTML content to extract bosses"""
        boss_data = {}
        # Find all occurrences of the href pattern
        npc_matches = re.finditer(r'href="/npc=(\d+)/', self.html_string)
        for match in npc_matches:
            npc_id = int(match.group(1))
            # Extract the substring that comes after the current match
            substring = self.html_string[match.end():]
            # Find the boss name in the substring
            name_match = re.search(r'>([^<]+)</a>', substring)
            if name_match:
                boss_name = name_match.group(1)
                boss_data[npc_id] = boss_name
        return boss_data

    def extract_item_ids(self) -> List[int]:
        """Parse HTML content to extract item IDs."""
        # Extract item IDs from the WH.Gatherer.addData section
        gatherer_data_pattern = r'WH\.Gatherer\.addData\(3, 1, ({.*?})\);'
        gatherer_data_match = re.search(gatherer_data_pattern, self.html_string, re.DOTALL)

        if gatherer_data_match:
            gatherer_data_str = gatherer_data_match.group(1)
            item_id_pattern = r'"(\d+)":\s*{'
            return re.findall(item_id_pattern, gatherer_data_str)
        return []

    def convert_to_json_and_save_to_disk(self) -> None:
        """Convert parsed data to JSON and save to disk."""
        json_str = json.dumps(self.parsed_data, indent=4)
        path = WowheadZone.folder / f"{self.zone_id}.json"
        ScrapeUtils.Persistence.write_textfile(path, json_str)


    @staticmethod
    def _set_trimmer_ruleset_for_wowhead_zone() -> None:
        """In ScrapeUtils.Trimmer, register trimming ruleset for wowhead.com/item"""
        target_url = "wowhead.com/zone="
        html_start = '<div class="text">'
        html_end = 'var tabsRelated = new Tabs'
        ScrapeUtils.Trimmer.register_trimming_ruleset(target_url, html_start, html_end)

    @staticmethod
    def scrape_wowhead_zone(zone_id: int) -> None:
        """Scrape zone data from Wowhead and save it."""
        WowheadZone._set_trimmer_ruleset_for_wowhead_zone()
        url = f"https://www.wowhead.com/zone={zone_id}"
        html_content = ScrapeUtils.Html.fetch_url(url)

        if len(html_content) != 0:
            wowhead_zone: WowheadZone = WowheadZone(zone_id, html_content)
            wowhead_zone.convert_to_json_and_save_to_disk()

class WowheadZoneList:
    """Represents a WoW zone with data scraped from Wowhead."""

    folder: Path = Path.cwd() / "wowhead_zone_list"
    instances: Dict[str, 'WowheadZoneList'] = {}

    def __init__(self, zone_list: str, html_string: str):
        """Initialize WowheadItem with item ID and HTML content."""
        self.zone_list: str = zone_list
        self.html_string: str = html_string
        self.parsed_data: Dict[str, Any] = {}
        self.parse(zone_list)
        WowheadZoneList.instances[zone_list] = self

    @classmethod
    def get_all_zone_ids(cls) -> List[int]:
        """
        Goes over each instance in the class variable 'instances' and returns a list of
        each zone_id found in parsed_data['zones'].keys()
        """
        all_zone_ids = []
        for instance in cls.instances.values():
            all_zone_ids.extend(instance.parsed_data['zones'].keys())
        return list(set(all_zone_ids))  # Remove duplicates and return as list

    def parse(self, zone_list: str) -> None:
        """Parse HTML content to extract item IDs."""
        self.parsed_data['zone_list'] = zone_list
        self.parsed_data['zones'] = self.extract_zone_list()

    def extract_zone_list(self) -> Dict[int, str]:
        """Parse each zone in the zone list in the html"""
        zone_data = {}

        # Find the data array in the JavaScript
        data_match = re.search(r'data: (\[.*?\])', self.html_string, re.DOTALL)
        if data_match:
            data_str = data_match.group(1)
            try:
                # Parse the JSON data
                data = json.loads(data_str)
                for zone in data:
                    if 'id' in zone and 'name' in zone:
                        zone_data[zone['id']] = zone['name']
            except json.JSONDecodeError:
                print("Error decoding JSON data")

        return zone_data

    def convert_to_json_and_save_to_disk(self) -> None:
        """Convert parsed data to JSON and save to disk."""
        json_str = json.dumps(self.parsed_data, indent=4)
        path = WowheadZone.folder / f"{self.zone_list}.json"
        ScrapeUtils.Persistence.write_textfile(path, json_str)

    @staticmethod
    def _set_trimmer_ruleset_for_wowhead_zone_list() -> None:
        """In ScrapeUtils.Trimmer, register trimming ruleset for wowhead.com/item"""
        target_url = "wowhead.com/zones"
        html_start = '<script type="text/javascript">//'
        html_end = '//]]></script>'
        ScrapeUtils.Trimmer.register_trimming_ruleset(target_url, html_start, html_end)

    @staticmethod
    def scrape_wowhead_zone_list(zone_list: str) -> None:
        """Scrape zone data from Wowhead and save it."""
        WowheadZoneList._set_trimmer_ruleset_for_wowhead_zone_list()
        url = f"https://www.wowhead.com/zones/{zone_list}"
        html_content = ScrapeUtils.Html.fetch_url(url)

        if len(html_content) != 0:
            wowhead_zone: WowheadZoneList = WowheadZoneList(zone_list, html_content)
            wowhead_zone.convert_to_json_and_save_to_disk()


if __name__ == "__main__":

    my_zone_list = "war-within/dungeons"
    WowheadZoneList.scrape_wowhead_zone_list(my_zone_list)
    every_zone_ids = WowheadZoneList.get_all_zone_ids()
    for my_zone in every_zone_ids:
        WowheadZone.scrape_wowhead_zone(my_zone)

    every_item_ids = WowheadZone.get_all_item_ids()
    for my_item in every_item_ids:
        WowheadItem.scrape_wowhead_item(my_item)

    WowheadItem.export_all_items_to_csv()
    print("Finished!")