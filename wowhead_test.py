import csv
import json
import re
from io import StringIO
from pathlib import Path
from typing import Optional, Set, Dict, Any, List

from scrape_utils import ScrapeUtils

class WowheadClassSpec:

    UNKNOWN_VALUE: str = "Unknown"
    primary_stats: Dict[str, str] = {
        "Agility": "Agi",
        "Strength": "Str",
        "Intellect": "Int",
    }
    secondary_stats: Dict[str, str] = {
        "Critical Strike": "Crit",
        "Haste": "Haste",
        "Mastery": "Mastery",
        "Versatility": "Vers",
    }
    slot_categories: Dict[str, List[str]] = {
        "Head": ["Head"],
        "Shoulders": ["Shoulder"],
        "Chest": ["Chest"],
        "Wrists": ["Wrist"],
        "Hands": ["Hands"],
        "Waist": ["Waist"],
        "Legs": ["Legs"],
        "Feet": ["Feet"],
        "Neck": ["Neck"],
        "Back": ["Back"],
        "Ring": ["Finger"],
        "Onehand": ["One-Hand", "Wand"],
        "Twohand": ["Two-Hand", "Ranged"],
        "Offhand": ["Held In Off-hand", "Off Hand"],
        "Trinket": ["Trinket"],
    }

    def __init__(self, class_name: str, spec_dict: Dict[int, str], short_names: Dict[str, str]):
        """Initialize a WowheadClassSpec instance."""
        self.name: str = class_name
        self.shortname: str = WowheadClassSpec.get_short_class_name(class_name)
        self.spec_dict: Dict[int, str] = spec_dict
        self.spec_shortened_names: Dict[str, str] = short_names

    def get_spec_ids(self) -> List[int]:
        spec_ids: List[int] = []
        for key in self.spec_dict:
            spec_ids.append(key)
        return spec_ids

    @staticmethod
    def death_knight() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Death Knight."""
        return WowheadClassSpec("Death Knight", {
            250: "Blood",
            251: "Frost",
            252: "Unholy"
        }, {
            "Blood": "Blood",
            "Frost": "Frost",
            "Unholy": "UH"
        })

    @staticmethod
    def demon_hunter() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Demon Hunter."""
        return WowheadClassSpec("Demon Hunter", {
            577: "Havoc",
            581: "Vengeance"
        }, {
            "Havoc": "Havoc",
            "Vengeance": "Veng"
        })

    @staticmethod
    def druid() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Druid."""
        return WowheadClassSpec("Druid", {
            102: "Balance",
            103: "Feral",
            104: "Guardian",
            105: "Restoration"
        }, {
            "Balance": "Owl",
            "Feral": "Cat",
            "Guardian": "Bear",
            "Restoration": "Resto"
        })

    @staticmethod
    def evoker() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Evoker."""
        return WowheadClassSpec("Evoker", {
            1467: "Devastation",
            1468: "Preservation",
            1473: "Augmentation"
        }, {
            "Devastation": "Dev",
            "Preservation": "Pres",
            "Augmentation": "Aug"
        })

    @staticmethod
    def hunter() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Hunter."""
        return WowheadClassSpec("Hunter", {
            253: "Beast Mastery",
            254: "Marksmanship",
            255: "Survival"
        }, {
            "Beast Mastery": "BM",
            "Marksmanship": "MM",
            "Survival": "SV"
        })

    @staticmethod
    def mage() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Mage."""
        return WowheadClassSpec("Mage", {
            62: "Arcane",
            63: "Fire",
            64: "Frost"
        }, {
            "Arcane": "Arc",
            "Fire": "Fire",
            "Frost": "Frost"
        })

    @staticmethod
    def monk() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Monk."""
        return WowheadClassSpec("Monk", {
            268: "Brewmaster",
            270: "Mistweaver",
            269: "Windwalker"
        }, {
            "Brewmaster": "Brew",
            "Mistweaver": "MW",
            "Windwalker": "WW"
        })

    @staticmethod
    def paladin() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Paladin."""
        return WowheadClassSpec("Paladin", {
            65: "Holy",
            66: "Protection",
            70: "Retribution"
        }, {
            "Holy": "Holy",
            "Protection": "Prot",
            "Retribution": "Ret"
        })

    @staticmethod
    def priest() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Priest."""
        return WowheadClassSpec("Priest", {
            256: "Discipline",
            257: "Holy",
            258: "Shadow"
        }, {
            "Discipline": "Disc",
            "Holy": "Holy",
            "Shadow": "Shad"
        })

    @staticmethod
    def rogue() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Rogue."""
        return WowheadClassSpec("Rogue", {
            259: "Assassination",
            260: "Outlaw",
            261: "Subtlety"
        }, {
            "Assassination": "Ass",
            "Outlaw": "Out",
            "Subtlety": "Sub"
        })

    @staticmethod
    def shaman() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Shaman."""
        return WowheadClassSpec("Shaman", {
            262: "Elemental",
            263: "Enhancement",
            264: "Restoration"
        }, {
            "Elemental": "Ele",
            "Enhancement": "Enh",
            "Restoration": "Resto"
        })

    @staticmethod
    def warlock() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Warlock."""
        return WowheadClassSpec("Warlock", {
            265: "Affliction",
            266: "Demonology",
            267: "Destruction"
        }, {
            "Affliction": "Aff",
            "Demonology": "Demo",
            "Destruction": "Dest"
        })

    @staticmethod
    def warrior() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for Warrior."""
        return WowheadClassSpec("Warrior", {
            71: "Arms",
            72: "Fury",
            73: "Protection"
        }, {
            "Arms": "Arms",
            "Fury": "Fury",
            "Protection": "Prot"
        })

    @staticmethod
    def unknown_class() -> 'WowheadClassSpec':
        """Return a WowheadClassSpec instance for an unknown class."""
        return WowheadClassSpec(WowheadClassSpec.UNKNOWN_VALUE, {}, {})


    @staticmethod
    def get_class_name(spec_id: int) -> str:
        """Get the class name for a given spec ID."""
        wow_class = WowheadClassSpec.get_wow_class(spec_id)
        return wow_class.name

    @staticmethod
    def get_short_class_name(class_name: str) -> str:
        """Get the short class name of a class"""
        if class_name == "Death Knight":
            return "DK"
        if class_name == "Demon Hunter":
            return "DH"
        return class_name

    @staticmethod
    def get_spec_name(spec_id: int) -> str:
        """Get the spec name for a given spec ID."""
        wow_class = WowheadClassSpec.get_wow_class(spec_id)
        return wow_class.spec_dict.get(spec_id, WowheadClassSpec.UNKNOWN_VALUE)

    @staticmethod
    def get_short_spec_name(spec_id: int) -> str:
        """Get the spec name for a given spec ID."""
        wow_class = WowheadClassSpec.get_wow_class(spec_id)
        spec_name = wow_class.spec_dict.get(spec_id, WowheadClassSpec.UNKNOWN_VALUE)
        return wow_class.spec_shortened_names.get(spec_name, WowheadClassSpec.UNKNOWN_VALUE)

    @staticmethod
    def get_name(spec_id: int) -> str:
        """Get the full name (spec + class) for a given spec ID."""
        class_name = WowheadClassSpec.get_class_name(spec_id)
        spec_name = WowheadClassSpec.get_spec_name(spec_id)
        return f"{spec_name} {class_name}"

    @staticmethod
    def get_camel_case_name(spec_id: int) -> str:
        class_name = WowheadClassSpec.get_class_name(spec_id)
        spec_name = WowheadClassSpec.get_spec_name(spec_id)
        return f"{class_name} {spec_name}".replace(" ", "")

    @staticmethod
    def get_camel_case_short_name(spec_id: int) -> str:
        class_name = WowheadClassSpec.get_class_name(spec_id)
        spec_name = WowheadClassSpec.get_short_spec_name(spec_id)
        return f"{class_name} {spec_name}".replace(" ", "")

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
        return wow_class.get_spec_ids()

    @staticmethod
    def get_all_specs_dict() -> Dict[int, str]:
        """Get a dictionary of all spec IDs and their corresponding spec names."""
        all_specs = {}
        for wow_class in WowheadClassSpec.get_all_classes():
            all_specs.update(wow_class.spec_dict)
        return all_specs

    @staticmethod
    def get_all_specs_list() -> List[int]:
        all_specs = WowheadClassSpec.get_all_specs_dict()
        return list(all_specs.keys())

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

    json_folder: Path = Path.cwd() / "wowhead_items"
    csv_folder: Path = Path.cwd() / "wowhead_item_csv"
    instances: Dict[int, 'WowheadItem'] = {}

    def __init__(self, item_id: int, html_string: str):
        """Initialize WowheadItem with item ID and HTML content."""
        self.item_id: int = item_id
        self.html_string: str = html_string
        self.parsed_data: Dict[str, Any] = {}
        self.parse(item_id)
        WowheadItem.instances[item_id] = self

    @staticmethod
    def register_instance(item_id: int, instance: 'WowheadItem') -> None:
        WowheadItem.instances[item_id] = instance

    @staticmethod
    def get_all_items_for_spec(spec_id: int) -> Set['WowheadItem']:
        items: Set['WowheadItem'] = set()
        for item in WowheadItem.instances.values():
            if not item.is_mount_or_quest_item():
                integer_spec_ids: List[int] = []
                for scraped_spec_id in item.parsed_data['spec_ids']:
                    if isinstance(scraped_spec_id, int):
                        integer_spec_ids.append(scraped_spec_id)
                    else:
                        print(f"{scraped_spec_id} is not an int. It is type {scraped_spec_id.type()}")
                if spec_id in integer_spec_ids:
                    items.add(item)
        return items

    @staticmethod
    def get_all_items_for_spec_and_slot(spec_id: int, slot_names: List[str]) -> Set['WowheadItem']:
        items: Set['WowheadItem'] = set()
        for slot_name in slot_names:
            for item in WowheadItem.get_all_items_for_spec(spec_id):
                if item.parsed_data['gear_slot'] == slot_name:
                    items.add(item)
        return items

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

    @staticmethod
    def parse_statistics_across_all_items_and_write_json() -> None:
        # Requires all other items to have been scraped already
        for item_id in WowheadZone.get_all_item_ids():
            item = WowheadItem.instances[item_id]
            item.add_statistic_item_drop_chance_per_spec()
            item.add_statistic_dungeon_name()
            item.add_statistic_boss_position()
            item.convert_to_json_and_save_to_disk()
        # Also save zones to json
        for zone_id in WowheadZoneList.get_all_zone_ids():
            zone = WowheadZone.instances[zone_id]
            zone.convert_to_json_and_save_to_disk()

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
        for stat in WowheadClassSpec.primary_stats:
            pattern = rf'\+([0-9,]+) \[?([^\]]*{stat}[^\]]*)\]?'
            value = self.extract_content(pattern)
            if value:
                # Remove commas and convert to integer
                stats[stat] = int(value.replace(',', ''))
        self.format_primary_stat_label(stats)
        return stats

    def format_primary_stat_label(self, stat_dict: Dict[str, int]) -> None:
        stats_found: List[str] = []
        for stat in WowheadClassSpec.primary_stats:
            if stat in stat_dict:
                stats_found.append(WowheadClassSpec.primary_stats[stat])
                self.parsed_data[stat.lower()] = f"{100}%"
            else:
                self.parsed_data[stat.lower()] = f"{0}%"
        if "Str" in stats_found and "Agi" in stats_found and "Int" in stats_found:
            self.parsed_data["mainstat"] = "All 3"
        else:
            self.parsed_data["mainstat"] = ",".join(stats_found)

    def extract_secondary_stats(self) -> Dict[str, int]:
        stats = {}
        for stat in WowheadClassSpec.secondary_stats:
            value = self.extract_content(rf'([0-9,]+) {stat}')
            if value:
                # Remove commas and convert to integer
                stats[stat] = int(value.replace(',', ''))
        self.extract_secondary_stat_distribution(stats)
        return stats

    def extract_secondary_stat_distribution(self, stat_dict: Dict[str, int]) -> None:
        total_stats = 0
        for stat in stat_dict:
            total_stats += stat_dict[stat]
        distribution: List[str] = []
        for stat in WowheadClassSpec.secondary_stats:
            if stat not in stat_dict or stat_dict[stat] == 0:
                self.parsed_data[stat.lower()] = f"{0}%"
            else:
                percent = f"{100 * stat_dict[stat] // total_stats}%"
                self.parsed_data[stat.lower()] = percent
                distribution.append(f"{percent} {WowheadClassSpec.secondary_stats[stat]}")
        distribution.sort(reverse=True)
        self.parsed_data["distribution"] = " + ".join(distribution)
        single_letter_distribution: List[str] = []
        for stat in distribution:
            if len(stat) >= 5:
                if stat[2] == "C" or stat[2] == "H" or stat[2] == "M" or stat[2] == "V":
                    single_letter_distribution.append(stat[2])
                elif stat[3] == "C" or stat[3] == "H" or stat[3] == "M" or stat[3] == "V":
                    single_letter_distribution.append(stat[3])
                elif stat[4] == "C" or stat[4] == "H" or stat[4] == "M" or stat[4] == "V":
                    single_letter_distribution.append(stat[4])
                else:
                    single_letter_distribution.append("")
        if self.parsed_data['gear_slot'] in ["One-Hand", "Wand", "Two-Hand", "Ranged", "Offhand",
                                             "Held In Off-hand", "Off Hand", "Trinket"]:
            self.parsed_data["stats"] = self.parsed_data["mainstat"] #Write mainstat type instead
        else:
            self.parsed_data["stats"] = ">".join(single_letter_distribution)

    def extract_sell_price(self) -> str:
        """Extract and format item sell price."""
        gold: Optional[str] = self.extract_content(r'<span class="moneygold">(\d+)</span>')
        silver: Optional[str] = self.extract_content(r'<span class="moneysilver">(\d+)</span>')
        copper: Optional[str] = self.extract_content(r'<span class="moneycopper">(\d+)</span>')
        return f"{gold or 0} gold, {silver or 0} silver, {copper or 0} copper"

    def extract_spec_ids(self) -> List[int]:
        """Extract spec IDs from the HTML content."""
        spec_ids = []
        pattern = r'<div class="iconsmall spec(\d+)"'
        matches = re.findall(pattern, self.html_string)
        for match in matches:
            spec_ids.append(int(match))
        if len(spec_ids) == 0:
            return WowheadClassSpec.get_all_specs_list()
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
        if len(self.html_string) == 0:
            print(f"Warning: Item {self.item_id} has empty html during json write...")
        json_str = json.dumps(self.parsed_data, indent=4)
        path = WowheadItem.json_folder / f"{self.item_id}.json"
        ScrapeUtils.Persistence.write_textfile(path, json_str)

    def add_statistic_dungeon_name(self) -> None:
        boss_name = self.parsed_data['dropped_by']
        dungeon_name = WowheadZone.get_boss_zone_name(boss_name)
        self.parsed_data['dropped_in'] = dungeon_name
        dungeon_short_name = WowheadZone.get_shortened_boss_zone_name(boss_name)
        self.parsed_data['from'] = dungeon_short_name

    def add_statistic_boss_position(self) -> None:
        boss_name = self.parsed_data['dropped_by']
        position = WowheadZone.get_boss_position(boss_name)
        self.parsed_data['boss_position'] = position

    def add_statistic_item_drop_chance_per_spec(self) -> None:
        spec_ids = WowheadClassSpec.get_all_specs_list()
        for spec_id in spec_ids:
            items = WowheadItem.get_all_items_for_spec(spec_id)
            boss_loot_table_size = 0
            for item in items:
                if spec_id in self.parsed_data['spec_ids']:
                    if item.parsed_data['dropped_by'] == self.parsed_data['dropped_by']:
                        if item.parsed_data['gear_slot'] is not None: #Ignore mounts/quest items
                            boss_loot_table_size += 1
            drop_chance = f"{0}%"
            if not boss_loot_table_size == 0:
                drop_chance = f"{100 // boss_loot_table_size}%"
            column_name = WowheadClassSpec.get_camel_case_short_name(spec_id)
            self.parsed_data[column_name] = drop_chance

    @staticmethod
    def sim_world_tour() -> None:
        for spec_id in WowheadClassSpec.get_all_specs_list():
            shortname = WowheadClassSpec.get_camel_case_short_name(spec_id)
            loot_chance = 0.2  # Chance of loot per player per boss
            gear_slots: Dict[str, str] = {}
            items = WowheadItem.get_all_items_for_spec(spec_id)
            for slot in WowheadClassSpec.slot_categories:
                chance_of_no_drops = 1.0
                items_considered = 0
                for item in items:
                    if item.parsed_data['gear_slot'] in WowheadClassSpec.slot_categories[slot]:
                        drop_chance = item.parsed_data[shortname].rstrip('%')
                        try:
                            drop_chance_float = float(drop_chance) / 100
                        except ValueError:
                            print(f"Warning: drop chance {drop_chance} is not numeric")
                            continue
                        chance_item_not_dropping = 1 - (loot_chance * drop_chance_float)
                        if drop_chance_float > 0:
                            chance_of_no_drops *= chance_item_not_dropping
                            items_considered += 1
                chance_of_at_least_one = (1 - chance_of_no_drops) * 100
                gear_slots[slot] = f"{chance_of_at_least_one:.2f}% ({items_considered} items)"
            print(f"{shortname}: {gear_slots}")

    @staticmethod
    def scrape_wowhead_item(item_id: int) -> None:
        """Scrape item data from Wowhead and save it."""
        WowheadItem._set_trimmer_ruleset_for_wowhead_items()
        url = f"https://www.wowhead.com/item={item_id}"
        html_content = ScrapeUtils.Html.fetch_url(url)

        if len(html_content) == 0:
            print(f"Warning: html_content is Empty for zone_id {item_id}")
        wowhead_item = WowheadItem(item_id, html_content)
        WowheadItem.register_instance(item_id, wowhead_item)

    def is_mount_or_quest_item(self) -> bool:
        return self.parsed_data['gear_slot'] == "" or self.parsed_data['gear_slot'] is None

    @staticmethod
    def _set_trimmer_ruleset_for_wowhead_items() -> None:
        """In ScrapeUtils.Trimmer, register trimming ruleset for wowhead.com/item"""
        target_url = "wowhead.com/item="
        html_start = '<h1 class="heading-size-1">'
        html_end = '<h2 class="heading-size-2 clear">Related</h2></div>'
        ScrapeUtils.Trimmer.register_trimming_ruleset(target_url, html_start, html_end)


class WowheadItemCsvExporter:

    @staticmethod
    def create_fixed_size_csv() -> None:
        """
        Create a fixed-size CSV file containing all class and slot combinations.
        Each combination is represented by a fixed-length row section in the CSV.
        """
        nested_csv_tables: Dict[str, List[WowheadItem]] = {}
        for wow_class in WowheadClassSpec.get_all_classes():
            for slot_category in WowheadClassSpec.slot_categories:
                items: List[WowheadItem] = []
                key = f"{wow_class.shortname} {slot_category}"
                for spec_id in wow_class.get_spec_ids():
                    wowhead_slot_names = WowheadClassSpec.slot_categories[slot_category]
                    items.extend(list(WowheadItem.get_all_items_for_spec_and_slot(int(spec_id), wowhead_slot_names)))

                # Sort items and ensure exactly a fixed_length number of items
                items = WowheadItemCsvExporter._sort_items(set(items))
                rows_for_slot = WowheadItemCsvExporter._decide_number_of_rows_in_fixed_csv(slot_category)
                if len(items) > rows_for_slot:
                    print(f"Warning: More than {rows_for_slot} items for {key}. Truncating to {rows_for_slot}.")
                    items = items[:rows_for_slot]
                elif len(items) < rows_for_slot:
                    empty_item = WowheadItem(-1, "")  # Create an empty item
                    empty_item.parsed_data = {key: "" for key in WowheadItemCsvExporter._sort_column_order(items)}
                    items.extend([empty_item] * (rows_for_slot - len(items)))

                nested_csv_tables[key] = items

        # Prepare CSV content
        csv_content = StringIO()
        writer = None

        for key, items in nested_csv_tables.items():
            if writer is None:
                columns = ['category'] + WowheadItemCsvExporter._sort_column_order(items)
                writer = csv.DictWriter(csv_content, fieldnames=columns, lineterminator='\n')
                writer.writeheader()

            i = 0
            for item in items:
                i += 1
                row_data = {'category': f"{key} #{i}"}
                row_data.update({col: item.parsed_data.get(col, '') for col in columns if col != 'category'})
                for col, value in row_data.items():
                    if isinstance(value, list):
                        row_data[col] = ', '.join(map(str, value))
                writer.writerow(row_data)

        # Save CSV file
        spreadsheet_csv_path = WowheadItem.csv_folder / "all_class_slots.csv"
        ScrapeUtils.Persistence.write_textfile(spreadsheet_csv_path, csv_content.getvalue())

    @staticmethod
    def export_items_to_csv_for_all_specs_and_classes() -> None:
        all_specs = WowheadClassSpec.get_all_specs_list()
        for wow_class in WowheadClassSpec.get_all_classes():
            class_spec_ids = wow_class.get_spec_ids()
            for spec_id in class_spec_ids:
                file_name = f"{WowheadClassSpec.get_camel_case_name(spec_id)}.csv"
                csv_path = WowheadItem.csv_folder / file_name
                WowheadItemCsvExporter._export_items_to_csv([spec_id], csv_path)
            wow_class_csv_path = WowheadItem.csv_folder / f"{wow_class.name.replace(' ', '')}.csv"
            WowheadItemCsvExporter._export_items_to_csv(class_spec_ids, wow_class_csv_path)
        all_specs_csv_path = WowheadItem.csv_folder / "all_items.csv"
        WowheadItemCsvExporter._export_items_to_csv(all_specs, all_specs_csv_path)

    @staticmethod
    def _export_items_to_csv(spec_ids: List[int], csv_path: Path) -> None:
        items = set()
        for spec_id in spec_ids:
            items.update(WowheadItem.get_all_items_for_spec(spec_id))
        if not items:
            print(f"Warning: No items found for csv {csv_path}. Creating CSV anyway...")
        # Filter out mounts and quest items
        sorted_items = WowheadItemCsvExporter._sort_items(items)
        columns = WowheadItemCsvExporter._sort_column_order(sorted_items)
        csv_content = StringIO()
        writer = csv.DictWriter(csv_content, fieldnames=columns, lineterminator='\n')
        writer.writeheader()
        for item in sorted_items:  # Write data for each item
            # Ensure all fields are present, use empty string for missing fields
            row_data = {key: item.parsed_data.get(key, '') for key in columns}
            for key, value in row_data.items():
                if isinstance(value, list):  # Convert lists to strings for CSV compatibility
                    row_data[key] = ', '.join(map(str, value))
            writer.writerow(row_data)
        ScrapeUtils.Persistence.write_textfile(csv_path, csv_content.getvalue())

    @staticmethod
    def _decide_number_of_rows_in_fixed_csv(slot_category: str) -> int:
        default = 0
        rows_for_armor_slots = 5
        rows_for_shared_slots = 5
        rows_for_rings = 8
        rows_for_weapon_slots = 10
        rows_for_trinkets = 24
        if slot_category == "Trinket":
            return rows_for_trinkets
        if slot_category == "Ring":
            return rows_for_rings
        weapons = ["Onehand", "Twohand", "Offhand"]
        if slot_category in weapons:
            return rows_for_weapon_slots
        shared_slots = ["Neck", "Back"]
        if slot_category in shared_slots:
            return rows_for_shared_slots
        armor_slots = ["Head", "Shoulders", "Chest", "Wrists", "Hands", "Waist", "Legs", "Feet"]
        if slot_category in armor_slots:
            return rows_for_armor_slots
        print(f"Error: {slot_category} did not match any known slot_category")
        return default

    @staticmethod
    def _sort_items(sorted_item_list: Set['WowheadItem']) -> List['WowheadItem']:
        return sorted(
            list(sorted_item_list),
            key=lambda x: (
                x.parsed_data.get('gear_slot') or '', # sort by slot #1 prio
                x.parsed_data.get('gear_type') or '', # sort by type #2 prio
                x.parsed_data.get('dropped_in') or '',
                x.parsed_data.get('boss_position') or ''
            )
        )

    @staticmethod
    def _sort_column_order(item_list: List['WowheadItem']) -> List[str]:
        columns: List[str] = []  # Create the fieldnames list with the desired order
        first_columns: List[str] = ['item_id', 'from', 'dropped_by',
                                    'boss_position', 'gear_slot', 'gear_type',
                                    'name', 'distribution', 'stats', 'mainstat']
        last_columns: List[str] = ['spec_ids', 'spec_names']
        columns.extend(first_columns)
        all_keys: Set[str] = set() # Get all unique keys from all items
        for item in item_list:
            all_keys.update(item.parsed_data.keys())
        middle_columns = sorted(list(all_keys - set(first_columns) - set(last_columns)))
        columns.extend(middle_columns)
        columns.extend(last_columns)
        return columns


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

    @staticmethod
    def register_instance(zone_id: int, instance: 'WowheadZone') -> None:
        WowheadZone.instances[zone_id] = instance

    @staticmethod
    def get_all_item_ids() -> List[int]:
        """
        Goes over each instance in the class variable 'instances' and returns a list of
        each item_id found in parsed_data['item_ids']
        """
        all_item_ids = []
        for instance in WowheadZone.instances.values():
            all_item_ids.extend(instance.parsed_data['item_ids'])
        return list(set(all_item_ids))  # Remove duplicates and return as list

    @staticmethod
    def get_boss_zone_name(boss_name: str) -> str:
        href_boss_name = WowheadZone.convert_boss_name_to_href_name(boss_name)
        if boss_name is not None:
            for zone in WowheadZone.instances.values():
                boss_order = zone.parsed_data['boss_order']
                for boss in boss_order:
                    if boss.lower() == boss_name.lower():
                        return zone.parsed_data['name']
                # Try alternative href names
                boss_hrefs = zone.parsed_data['boss_hrefs']
                for href in boss_hrefs:
                    if href_boss_name == href:
                        return zone.parsed_data['name']
        return WowheadClassSpec.UNKNOWN_VALUE

    @staticmethod
    def get_shortened_boss_zone_name(boss_name: str) -> str:
        name_length_limit = 16
        boss_zone_name = WowheadZone.get_boss_zone_name(boss_name)
        if boss_zone_name.startswith("The "):
            boss_zone_name = boss_zone_name[4:]
        if len(boss_zone_name) > name_length_limit and "," in boss_zone_name:
            boss_zone_name = boss_zone_name.split(",")[0]
        if len(boss_zone_name) <= name_length_limit:
            return boss_zone_name
        words = boss_zone_name.split()
        while len(' '.join(words)) > name_length_limit and len(words) > 1:
            words.pop()
        while len(words) > 1 and not words[-1].istitle():
            words.pop()
        shortened_name = ' '.join(words)
        return shortened_name

    @staticmethod
    def get_boss_position(boss_name: str) -> str:
        href_boss_name = WowheadZone.convert_boss_name_to_href_name(boss_name)
        if boss_name is not None:
            for zone in WowheadZone.instances.values():
                boss_order = zone.parsed_data['boss_order']
                for index, boss in enumerate(boss_order):
                    if boss.lower() == boss_name.lower():
                        return f"{index + 1} of {len(boss_order)}"
                # Try alternative href names
                boss_hrefs = zone.parsed_data['boss_hrefs']
                for index, href in enumerate(boss_hrefs):
                    if href_boss_name == href:
                        return f"{index + 1} of {len(boss_hrefs)}"
            return "? of ?"

    def parse(self, zone_id: int) -> None:
        """Parse HTML content to extract item IDs."""
        self.parsed_data['zone_id'] = zone_id
        self.parsed_data['name'] = self.extract_name()
        self.parsed_data['bosses'] = self.extract_bosses()
        self.parsed_data['item_ids'] = self.extract_item_ids()
        boss_order = [boss['display_name'] for boss in self.parsed_data['bosses'].values()]
        self.parsed_data['boss_order'] = boss_order
        boss_href_name = [boss['href_name'] for boss in self.parsed_data['bosses'].values()]
        self.parsed_data['boss_hrefs'] = boss_href_name

    def extract_name(self) -> str:
        pattern = r'var myMapper = new Mapper\({"parent":"[^"]+","zone":\d+,"name":"([^"]+)"\}\);'
        match = re.search(pattern, self.html_string)
        return match.group(1) if match else "Unknown"

    def extract_bosses(self) -> Dict[str, Dict[str, str]]:
        """Parse HTML content to extract bosses"""
        boss_data: Dict[str, Dict[str, str]] = {}
        # Find all <li> elements containing boss information
        li_elements = re.findall(r'<li><div><a[^>]*>.*?</a>.*?</div></li>', self.html_string, re.DOTALL)

        for li in li_elements:
            # Extract npc_id, href_name, and display_name
            npc_match = re.search(r'href="/npc=(\d+)/([^"]+)"', li)
            name_match = re.search(r'<a[^>]*>([^<]+)</a>', li)
            if npc_match:
                npc_id = int(npc_match.group(1))
                href_name = npc_match.group(2)
            else:
                npc_id = -1
                href_name = WowheadClassSpec.UNKNOWN_VALUE

            display_name = name_match.group(1) if name_match else None
            if href_name == WowheadClassSpec.UNKNOWN_VALUE:
                if isinstance(display_name, str):
                    href_name = WowheadZone.convert_boss_name_to_href_name(display_name)
            if display_name:
                boss_data[display_name] = {
                    'npc_id': str(npc_id),
                    'href_name': href_name,
                    'display_name': display_name
                }
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
        if len(self.html_string) == 0:
            print(f"Warning: Item {self.zone_id} has empty html during json write...")
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

        if len(html_content) == 0:
            print(f"Warning: html_content is Empty for zone_id {zone_id}")
        wowhead_zone = WowheadZone(zone_id, html_content)
        WowheadZone.register_instance(zone_id, wowhead_zone)

    @staticmethod
    def convert_boss_name_to_href_name(boss_name: Optional[str]) -> str:
        if boss_name is None:
            return WowheadClassSpec.UNKNOWN_VALUE
        href_name = boss_name.lower()# Convert to lowercase
        return re.sub(r'[^a-z0-9]+', '-', href_name) #replace special chars with hyphen

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

    @staticmethod
    def register_instance(zone_list: str, instance: 'WowheadZoneList') -> None:
        WowheadZoneList.instances[zone_list] = instance

    @staticmethod
    def get_all_zone_ids() -> List[int]:
        """
        Goes over each instance in the class variable 'instances' and returns a list of
        each zone_id found in parsed_data['zones'].keys()
        """
        all_zone_ids = []
        for instance in WowheadZoneList.instances.values():
            all_zone_ids.extend(instance.parsed_data['zones'].keys())
        return list(set(all_zone_ids))  # Remove duplicates and return as list

    def parse(self, zone_list: str) -> None:
        """Parse HTML content to extract item IDs."""
        self.parsed_data['zone_list'] = zone_list
        self.parsed_data['zones'] = self.extract_zone_list()
        self.convert_to_json_and_save_to_disk()

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
        if len(self.html_string) == 0:
            print(f"Warning: Item {self.zone_list} has empty html during json write...")
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

        if len(html_content) == 0:
            print(f"Warning: html_content is Empty for zone_id {zone_list}")
        wowhead_zone_list = WowheadZoneList(zone_list, html_content)
        WowheadZoneList.register_instance(zone_list, wowhead_zone_list)


if __name__ == "__main__":

    my_zone_list = "war-within/dungeons"
    WowheadZoneList.scrape_wowhead_zone_list(my_zone_list)
    every_zone_ids = WowheadZoneList.get_all_zone_ids()
    for my_zone in every_zone_ids:
        WowheadZone.scrape_wowhead_zone(my_zone)

    every_item_ids = WowheadZone.get_all_item_ids()
    for my_index, my_item in enumerate(every_item_ids):
        #print(f"Scraping {my_item} ({my_index+1} of {len(every_item_ids)})")
        WowheadItem.scrape_wowhead_item(my_item)

    print("Creating item jsons...")
    WowheadItem.parse_statistics_across_all_items_and_write_json()

    print("Building csv files...")
    WowheadItemCsvExporter.export_items_to_csv_for_all_specs_and_classes()
    WowheadItemCsvExporter.create_fixed_size_csv()

    WowheadItem.sim_world_tour()
    print("Finished!")
