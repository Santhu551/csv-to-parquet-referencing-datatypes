#!/usr/bin/env python3
"""
Standalone IBM Optim XML -> Archive Viewer datatypes JSON generator.

Usage:
    python3 generate_datatypes.py <xml_folder> <output_folder>

Example:
    python3 generate_datatypes.py /home/postgres/xml_files /home/postgres/output

For every XML file:
    BlobClob.xml
        -> output/BlobClob_datatypes.json
        -> output/BlobClob_logs/BlobClob.log
        -> output/BlobClob_logs/BlobClob_errors.json

    Customer.xml
        -> output/Customer_datatypes.json
        -> output/Customer_logs/Customer.log
        -> output/Customer_logs/Customer_errors.json

No AF file is required.
AF validation is intentionally disabled.

External Python packages are NOT required.
"""

import argparse
import json
import logging
import sys
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# Archive Viewer behavior:
# The supplied standalone requirement is to use append_schema_name=True by default.
APPEND_SCHEMA_NAME = True  # Archive Viewer property used by the standalone tool

# SQL Server vendorVersion values used by Archive Viewer.
SQL_SERVER_VENDOR_VERSIONS = {
    "8.0 (2000)",
    "2005",
    "2008, 2012(Toleration), 2014(Toleration)",
    "2016",
    "2017, 2019, 2022, Azure",
}


def local_name(tag: str) -> str:
    """Return XML tag name without namespace/prefix."""
    if not tag:
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    if ":" in tag:
        return tag.rsplit(":", 1)[-1]
    return tag


def first(element: ET.Element, name: str) -> Optional[ET.Element]:
    """Find the first descendant element by namespace-independent name."""
    wanted = name.lower()
    for child in element.iter():
        if local_name(child.tag).lower() == wanted:
            return child
    return None


def children(element: ET.Element, name: str) -> List[ET.Element]:
    """Return direct children matching name, ignoring XML namespace."""
    wanted = name.lower()
    return [
        child
        for child in list(element)
        if local_name(child.tag).lower() == wanted
    ]


def descendants(element: ET.Element, name: str) -> List[ET.Element]:
    """Return all descendants matching name, ignoring XML namespace."""
    wanted = name.lower()
    return [
        child
        for child in element.iter()
        if local_name(child.tag).lower() == wanted
    ]


def attr(element: Optional[ET.Element], name: str, default=None):
    """Read an XML attribute safely."""
    if element is None:
        return default

    # Normal attribute lookup first.
    if name in element.attrib:
        return element.attrib[name]

    # Be tolerant of namespaced attributes.
    wanted = name.lower()
    for key, value in element.attrib.items():
        if local_name(key).lower() == wanted:
            return value

    return default


def setup_logger(log_file: Path) -> logging.Logger:
    """Create a dedicated logger for one XML file."""
    logger_name = f"datatypes_generator.{log_file.stem}.{id(log_file)}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console output is useful when running from Linux shell.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def validate(root: ET.Element) -> None:
    """
    Validate only data that the generator must have to create a valid
    Archive Viewer datatypes JSON.

    This is NOT validation against a sample XML, expected JSON, AF file,
    table name, GUID, customer, database, or known test case.
    """
    archive_file = first(root, "archiveFile")
    if archive_file is None:
        raise ValueError("archiveFile element not found")

    # The GUID is the top-level JSON key in Archive Viewer output.
    get_guid(archive_file)

    entities = first(root, "entities")
    if entities is None:
        raise ValueError("entities element not found")

    table_entities = [
        e for e in children(entities, "entity")
        if (attr(e, "type", "") or "").lower() == "table"
    ]

    if not table_entities:
        raise ValueError("No Table entity found in XML")

    for entity in table_entities:
        table_name = attr(entity, "name")
        if not table_name:
            raise ValueError("Table entity has no name")

        column_list = first(entity, "columnList")
        if column_list is None:
            raise ValueError(f"columnList not found for table '{table_name}'")

        for column in children(column_list, "column"):
            if not attr(column, "name"):
                raise ValueError(f"Column without name in table '{table_name}'")
            if not attr(column, "dataType"):
                raise ValueError(
                    f"Column '{attr(column, 'name')}' in table '{table_name}' has no dataType"
                )


def get_guid(archive_file: ET.Element) -> str:
    """Get Archive File GUID."""
    guid = attr(archive_file, "fileGUID") or attr(archive_file, "guid")

    if not guid:
        guid_element = first(archive_file, "guid")
        if guid_element is not None and guid_element.text:
            guid = guid_element.text.strip()

    if not guid:
        raise ValueError("Archive File GUID is missing")

    return guid


def get_archive_file_details(archive_file: ET.Element) -> Dict:
    """Map archiveFile attributes to Archive Viewer JSON keys."""
    mapping = {
        "creationDate": "AF_CREATION_DATE",
        "dataByteCount": "AF_DATABYTE_COUNT",
        "entityCount": "AF_ENTITY_COUNT",
        "secured": "AF_SECURE",
        "uncompressedFileSize": "AF_UNCOMPRESSED_FILESIZE",
        "compressionRatio": "AF_COMPRESSION_RATIO",
        "totalRecords": "AF_TOTAL_RECORDS",
        "encrypted": "AF_ENCRYPTED",
        "compressed": "AF_COMPRESSED",
        "totalSizeInBytes": "AF_TOTALSIZE_INBYTES",
    }

    result = {}

    for xml_attr, json_key in mapping.items():
        result[json_key] = attr(archive_file, xml_attr)

    return result


def get_native_lob_present(access_definition: Optional[ET.Element]) -> bool:
    """
    Archive Viewer logic:

        find_table_tag = defination_info.find("tables")
        NATIVE_LOB_PRESENT = find_table_tag is None

    Important:
    This is based on the presence of <tables> directly under
    <accessDefinition>, not on whether BLOB/CLOB columns exist.
    """
    if access_definition is None:
        return True

    tables = children(access_definition, "tables")
    return len(tables) == 0


def get_sql_server_database(root: ET.Element) -> bool:
    """
    Reproduce Archive Viewer's SQL_SERVER_DATABASE check.

    Archive Viewer reads the dataStoreAlias vendorVersion and compares it
    against its supported SQL Server version strings. It does not infer this
    flag from directory.databaseVendor.
    """
    access_definition = first(root, "accessDefinition")
    alias = None

    if access_definition is not None:
        direct_aliases = children(access_definition, "dataStoreAlias")
        if direct_aliases:
            alias = direct_aliases[0]

    # Some Optim XML variants place dataStoreAlias outside accessDefinition.
    # Use the first one only, matching the single-value behavior expected by
    # the Archive Viewer metadata generator rather than scanning all aliases.
    if alias is None:
        aliases = descendants(root, "dataStoreAlias")
        if aliases:
            alias = aliases[0]

    vendor_version = attr(alias, "vendorVersion") if alias is not None else None
    return vendor_version in SQL_SERVER_VENDOR_VERSIONS


def split_qualified_table_name(name: str) -> Tuple[str, str, str]:
    """
    Split a fully qualified table name.

    Expected form:
        DATABASE.SCHEMA.TABLE

    The original Archive Viewer logic uses the first, second and last
    components for table naming.
    """
    parts = [p.strip().strip('"') for p in name.split(".") if p.strip()]

    if len(parts) == 1:
        return "", "", parts[0]

    if len(parts) == 2:
        return parts[0], parts[1], parts[1]

    return parts[0], parts[1], parts[-1]


def table_output_name(
    qualified_name: str,
    default_qualifier: str,
    append_schema_name: bool,
) -> str:
    """
    Reproduce the table naming branches of the Archive Viewer generator.
    """
    parts = [p.strip().strip('"') for p in qualified_name.split(".") if p.strip()]

    if not parts:
        raise ValueError(f"Invalid table name: '{qualified_name}'")

    table_name = parts[-1]

    if len(parts) == 1:
        return table_name

    first_part = parts[0]
    second_part = parts[1]
    schema_db_name = f"{first_part}.{second_part}"

    qualifier = (default_qualifier or "").replace('"', "").strip()

    if not append_schema_name:
        if schema_db_name == qualifier:
            return table_name

        if len(parts) >= 3:
            return f"{first_part}_{second_part}_{parts[-1]}"

        return f"{first_part}_{second_part}"

    # append_schema_name == True
    if schema_db_name == qualifier:
        return table_name

    if len(parts) >= 3:
        return f"{second_part}.{parts[-1]}"

    return f"{second_part}.{parts[-1]}"


def datatype_with_precision_scale(column: ET.Element) -> str:
    """Build NUMBER/NUMERIC/DECIMAL datatype with precision/scale."""
    data_type = attr(column, "dataType", "")

    if not data_type:
        raise ValueError(
            f"Column '{attr(column, 'name', '<unknown>')}' has no dataType"
        )

    upper_type = data_type.upper()

    if upper_type in {"NUMBER", "NUMERIC", "DECIMAL"}:
        precision = attr(column, "precision")
        scale = attr(column, "scale")

        if precision not in (None, "") or scale not in (None, ""):
            precision_text = "" if precision is None else str(precision)
            scale_text = "" if scale is None else str(scale)
            return f"{upper_type}({precision_text},{scale_text})"

    return data_type


def build_table_info(
    root: ET.Element,
    default_qualifier: str,
    append_schema_name: bool,
    native_lob_present: bool,
) -> Tuple[Dict, Dict[str, str]]:
    """
    Build TABLE_INFO and retain mapping of qualified table names to
    output table names.

    Archive Viewer behavior:
    - TBL_ROW_COUNT and TBL_DATA_BYTE_COUNT are stored first.
    - When NATIVE_LOB_PRESENT is false, every TABLE_INFO entry gets
      a LOB_COLUMNS key.
    - LOB_COLUMNS contains only accessDefinition <columns nativeLOB="Y">
      columns for that table.
    - If no native LOB columns exist, LOB_COLUMNS is {}.
    """
    table_info = {}
    qualified_to_output = {}

    entities = first(root, "entities")
    if entities is None:
        return table_info, qualified_to_output

    lob_columns_by_table = {}

    if not native_lob_present:
        access_definition = first(root, "accessDefinition")
        if access_definition is not None:
            for tables in children(access_definition, "tables"):
                access_table_name = attr(tables, "name")
                if not access_table_name:
                    continue

                lob_columns = {}
                for column in children(tables, "columns"):
                    if str(attr(column, "nativeLOB", "")).upper() == "Y":
                        column_name = attr(column, "name")
                        if column_name:
                            lob_columns[column_name] = []

                lob_columns_by_table[access_table_name] = lob_columns

    table_entities = [
        e for e in children(entities, "entity")
        if (attr(e, "type", "") or "").lower() == "table"
    ]

    used_names = {}

    for entity in table_entities:
        qualified_name = attr(entity, "name")
        if not qualified_name:
            continue

        output_name = table_output_name(
            qualified_name,
            default_qualifier,
            append_schema_name,
        )

        if output_name in used_names:
            parts = [
                p.strip().strip('"')
                for p in qualified_name.split(".")
                if p.strip()
            ]

            if len(parts) >= 3:
                output_name = f"{parts[0]}_{parts[1]}_{parts[-1]}"

            if output_name in used_names:
                counter = 2
                base = output_name
                while f"{base}_{counter}" in used_names:
                    counter += 1
                output_name = f"{base}_{counter}"

        used_names[output_name] = qualified_name
        qualified_to_output[qualified_name] = output_name

        table_data = {
            "TBL_ROW_COUNT": attr(entity, "recordCount"),
            "TBL_DATA_BYTE_COUNT": attr(entity, "dataByteCount"),
        }

        # This is the important Archive Viewer structure:
        # LOB_COLUMNS is INSIDE each TABLE_INFO table.
        if not native_lob_present:
            short_table_name = qualified_name.split(".")[-1].strip('"')
            lob_value = (
                lob_columns_by_table.get(short_table_name)
                or lob_columns_by_table.get(qualified_name)
                or {}
            )
            table_data["LOB_COLUMNS"] = dict(lob_value)

        column_list = first(entity, "columnList")
        if column_list is not None:
            for column in children(column_list, "column"):
                column_name = attr(column, "name")
                if not column_name:
                    continue

                table_data[column_name] = datatype_with_precision_scale(column)

        table_info[output_name] = table_data

    return table_info, qualified_to_output


def apply_file_attachments(
    root: ET.Element,
    table_info: Dict,
    qualified_to_output: Dict[str, str],
) -> None:
    """
    Handle:
        <fileAttachments prefix="X">
            ...
        </fileAttachments>

    resulting in:
        X_BLOB: "BLOB"
    """
    for tables in descendants(root, "tables"):
        for table in descendants(tables, "table"):
            table_name = (
                attr(table, "name")
                or attr(table, "tableName")
                or attr(table, "qualifiedName")
            )

            if not table_name:
                continue

            output_name = qualified_to_output.get(table_name)

            if output_name is None:
                candidate = table_name.split(".")[-1].strip('"')
                for existing_name in table_info:
                    if existing_name.split(".")[-1].strip('"') == candidate:
                        output_name = existing_name
                        break

            if output_name is None:
                continue

            for attachment in descendants(table, "fileAttachments"):
                prefix = attr(attachment, "prefix")
                if prefix:
                    table_info[output_name][f"{prefix}_BLOB"] = "BLOB"


def generate(xml_file: Path, logger: logging.Logger):
    """Generate one Archive Viewer-compatible entry from one XML file."""
    logger.info("Reading XML: %s", xml_file)

    tree = ET.parse(xml_file)
    root = tree.getroot()

    validate(root)

    archive_file = first(root, "archiveFile")
    if archive_file is None:
        raise ValueError("archiveFile element not found")

    guid = get_guid(archive_file)
    logger.info("Archive GUID: %s", guid)

    result = get_archive_file_details(archive_file)

    ilg = first(root, "ilg")
    if ilg is not None:
        result["AF_STORAGE_PROFILE"] = attr(ilg, "storageProfile")
        result["AF_HOLDING_DELETE"] = attr(ilg, "holdingDelete")

    access_definition = first(root, "accessDefinition")

    if access_definition is not None:
        result["AF_DEFAULT_QUALIFIER"] = attr(
            access_definition,
            "defaultQualifier",
        )

    default_qualifier = result.get("AF_DEFAULT_QUALIFIER") or ""

    native_lob_present = get_native_lob_present(access_definition)
    result["NATIVE_LOB_PRESENT"] = native_lob_present

    result["SQL_SERVER_DATABASE"] = get_sql_server_database(root)

    logger.info(
        "NATIVE_LOB_PRESENT: %s | SQL_SERVER_DATABASE: %s",
        native_lob_present,
        result["SQL_SERVER_DATABASE"],
    )

    table_info, qualified_to_output = build_table_info(
        root,
        default_qualifier,
        APPEND_SCHEMA_NAME,
        native_lob_present,
    )

    if not table_info:
        raise ValueError("No table information generated")

    result["TABLE_INFO"] = table_info

    apply_file_attachments(
        root,
        table_info,
        qualified_to_output,
    )

    logger.info("TABLE_INFO tables: %d", len(table_info))

    return guid, result


def create_output_paths(xml_file: Path, output_folder: Path):
    """
    Create isolated output paths for one XML.

    Example:
        BlobClob.xml

        output/
        +-- BlobClob_datatypes.json
        +-- BlobClob_logs/
            +-- BlobClob.log
            +-- BlobClob_errors.json
    """
    base_name = xml_file.stem

    datatypes_file = output_folder / f"{base_name}_datatypes.json"

    log_folder = output_folder / f"{base_name}_logs"
    log_folder.mkdir(parents=True, exist_ok=True)

    log_file = log_folder / f"{base_name}.log"
    error_file = log_folder / f"{base_name}_errors.json"

    return datatypes_file, log_folder, log_file, error_file


def process_xml(xml_file: Path, output_folder: Path) -> bool:
    """Process one XML completely independently."""
    datatypes_file, log_folder, log_file, error_file = create_output_paths(
        xml_file,
        output_folder,
    )

    logger = setup_logger(log_file)

    try:
        logger.info("=" * 80)
        logger.info("STARTING XML PROCESSING")
        logger.info("XML FILE: %s", xml_file)
        logger.info("OUTPUT JSON: %s", datatypes_file)
        logger.info("LOG FOLDER: %s", log_folder)
        logger.info("ERROR FILE: %s", error_file)
        logger.info("AF validation: DISABLED")
        logger.info("APPEND_SCHEMA_NAME: %s", APPEND_SCHEMA_NAME)

        guid, data = generate(xml_file, logger)

        final_json = {guid: data}

        with datatypes_file.open("w", encoding="utf-8") as f:
            json.dump(final_json, f, indent=4, ensure_ascii=False)

        # Successful processing: remove an old error file if it exists.
        if error_file.exists():
            error_file.unlink()

        logger.info("SUCCESS: datatypes JSON generated")
        logger.info("SUCCESS FILE: %s", datatypes_file)
        logger.info("PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        return True

    except Exception as exc:
        error_data = {
            "xml_file": str(xml_file),
            "status": "FAILED",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
        }

        with error_file.open("w", encoding="utf-8") as f:
            json.dump(error_data, f, indent=4, ensure_ascii=False)

        logger.error("FAILED: %s", xml_file.name)
        logger.error("%s: %s", type(exc).__name__, exc)
        logger.error("ERROR FILE: %s", error_file)
        logger.error("PROCESSING FAILED")
        logger.info("=" * 80)

        return False

    finally:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Archive Viewer datatypes JSON from XML files."
    )

    parser.add_argument(
        "xml_folder",
        help="Folder containing XML files.",
    )

    parser.add_argument(
        "output_folder",
        help="Folder where generated JSON and per-file logs will be created.",
    )

    args = parser.parse_args()

    xml_folder = Path(args.xml_folder).expanduser().resolve()
    output_folder = Path(args.output_folder).expanduser().resolve()

    if not xml_folder.exists():
        print(f"ERROR: XML folder does not exist: {xml_folder}")
        return 1

    if not xml_folder.is_dir():
        print(f"ERROR: XML path is not a folder: {xml_folder}")
        return 1

    output_folder.mkdir(parents=True, exist_ok=True)

    xml_files = sorted(
        p for p in xml_folder.iterdir()
        if p.is_file() and p.suffix.lower() == ".xml"
    )

    if not xml_files:
        print(f"ERROR: No XML files found in: {xml_folder}")
        return 1

    print(f"Found {len(xml_files)} XML file(s)")
    print(f"Output folder: {output_folder}")

    successful = 0
    failed = 0

    for xml_file in xml_files:
        if process_xml(xml_file, output_folder):
            successful += 1
        else:
            failed += 1

    print()
    print("=" * 60)
    print("PROCESSING SUMMARY")
    print("=" * 60)
    print(f"Total XML files : {len(xml_files)}")
    print(f"Successful      : {successful}")
    print(f"Failed          : {failed}")
    print(f"Output folder   : {output_folder}")
    print("=" * 60)

    # 0 = everything successful
    # 2 = one or more XML files failed
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
