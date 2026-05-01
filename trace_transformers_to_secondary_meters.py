import argparse
import csv
import json
import os
import sys
import tempfile
from typing import Dict, List, Optional, Sequence, Set, Tuple

import arcpy


TRANSFORMER_TYPE_NAMES = ("Overhead Transformer", "Underground Transformer")
METER_TYPE_NAMES = ("Secondary Meter",)


def log(message: str) -> None:
    print(message)
    try:
        arcpy.AddMessage(message)
    except Exception:
        pass


def warn(message: str) -> None:
    print(f"WARNING: {message}")
    try:
        arcpy.AddWarning(message)
    except Exception:
        pass


def fail(message: str) -> None:
    try:
        arcpy.AddError(message)
    except Exception:
        pass
    raise RuntimeError(message)


def normalize_guid(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    if text.startswith("{") and text.endswith("}"):
        return text
    return "{" + text.strip("{}") + "}"


def ensure_directory(path: str) -> None:
    folder = os.path.dirname(path)
    if folder and not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)


def ensure_file_gdb(gdb_path: str) -> None:
    if arcpy.Exists(gdb_path):
        return
    folder = os.path.dirname(gdb_path)
    name = os.path.basename(gdb_path)
    if not name.lower().endswith(".gdb"):
        fail(f"Scratch workspace must be a file geodatabase path: {gdb_path}")
    os.makedirs(folder, exist_ok=True)
    arcpy.management.CreateFileGDB(folder, name)


def find_field(feature_class: str, candidates: Sequence[str]) -> str:
    fields = {field.name.upper(): field.name for field in arcpy.ListFields(feature_class)}
    for candidate in candidates:
        match = fields.get(candidate.upper())
        if match:
            return match
    fail(f"None of the expected fields were found in {feature_class}: {', '.join(candidates)}")


def quote_sql(value: object) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def get_asset_type_pairs(feature_class: str, target_type_names: Sequence[str]) -> List[Dict[str, object]]:
    subtype_field = arcpy.Describe(feature_class).subtypeFieldName
    if not subtype_field:
        fail(f"{feature_class} does not expose a subtype field. This script expects ASSETGROUP/ASSETTYPE-style utility network classes.")

    wanted = {name.strip().lower() for name in target_type_names}
    subtype_map = arcpy.da.ListSubtypes(feature_class)
    matches: List[Dict[str, object]] = []

    for subtype_code, subtype_info in subtype_map.items():
        field_values = subtype_info.get("FieldValues", {})
        assettype_info = field_values.get("ASSETTYPE")
        if not assettype_info:
            continue

        domain = assettype_info[1]
        if not domain or not hasattr(domain, "codedValues"):
            continue

        for asset_type_code, asset_type_name in domain.codedValues.items():
            if str(asset_type_name).strip().lower() in wanted:
                matches.append(
                    {
                        "asset_group_code": subtype_code,
                        "asset_group_name": subtype_info.get("Name", str(subtype_code)),
                        "asset_type_code": asset_type_code,
                        "asset_type_name": asset_type_name,
                    }
                )

    if not matches:
        fail(
            "No matching asset types were found in "
            f"{feature_class} for: {', '.join(target_type_names)}"
        )

    return matches


def build_pair_where_clause(
    feature_class: str,
    pairs: Sequence[Dict[str, object]],
    asset_group_field: str,
    asset_type_field: str,
) -> str:
    ag = arcpy.AddFieldDelimiters(feature_class, asset_group_field)
    at = arcpy.AddFieldDelimiters(feature_class, asset_type_field)
    parts = []
    for pair in pairs:
        parts.append(
            f"({ag} = {quote_sql(pair['asset_group_code'])} AND "
            f"{at} = {quote_sql(pair['asset_type_code'])})"
        )
    return " OR ".join(parts)


def make_pair_lookup(pairs: Sequence[Dict[str, object]]) -> Dict[Tuple[int, int], Dict[str, object]]:
    return {
        (int(pair["asset_group_code"]), int(pair["asset_type_code"])): pair
        for pair in pairs
    }


def build_feature_lookup(
    feature_class: str,
    pairs: Sequence[Dict[str, object]],
    label_prefix: str,
) -> Tuple[List[Dict[str, object]], Dict[str, Dict[str, object]]]:
    globalid_field = find_field(feature_class, ("GLOBALID",))
    objectid_field = arcpy.Describe(feature_class).OIDFieldName
    asset_group_field = find_field(feature_class, ("ASSETGROUP",))
    asset_type_field = find_field(feature_class, ("ASSETTYPE",))
    where_clause = build_pair_where_clause(feature_class, pairs, asset_group_field, asset_type_field)
    pair_lookup = make_pair_lookup(pairs)

    rows: List[Dict[str, object]] = []
    by_guid: Dict[str, Dict[str, object]] = {}

    fields = [objectid_field, globalid_field, asset_group_field, asset_type_field]
    with arcpy.da.SearchCursor(feature_class, fields, where_clause=where_clause) as cursor:
        for object_id, global_id, asset_group_code, asset_type_code in cursor:
            guid = normalize_guid(global_id)
            pair = pair_lookup.get((int(asset_group_code), int(asset_type_code)))
            if not pair:
                continue

            row = {
                "object_id": object_id,
                "global_id": guid,
                "asset_group_code": int(asset_group_code),
                "asset_group_name": str(pair["asset_group_name"]),
                "asset_type_code": int(asset_type_code),
                "asset_type_name": str(pair["asset_type_name"]),
                "label": f"{label_prefix} {object_id}",
            }
            rows.append(row)
            by_guid[guid] = row

    return rows, by_guid


def create_single_starting_point(
    utility_network: str,
    transformer_class: str,
    transformer_globalid: str,
    terminal_value: object,
    output_feature_class: str,
) -> None:
    arcpy.env.outputZFlag = "Enabled"
    trace_locations = [[transformer_class, transformer_globalid, terminal_value, None]]
    arcpy.un.AddTraceLocations(
        utility_network,
        output_feature_class,
        "DO_NOT_LOAD_SELECTED_FEATURES",
        "CLEAR_LOCATIONS",
        trace_locations,
        "TRAVERSABILITY_BARRIER",
    )


def collect_meter_guids_from_trace_json(
    json_path: str,
    allowed_meter_pairs: Set[Tuple[int, int]],
) -> Set[str]:
    with open(json_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    feature_elements = payload.get("featureElements", [])
    meter_guids: Set[str] = set()

    for element in feature_elements:
        asset_group_code = element.get("assetGroupCode")
        asset_type_code = element.get("assetTypeCode")
        global_id = element.get("globalId")

        if asset_group_code is None or asset_type_code is None or global_id is None:
            continue

        key = (int(asset_group_code), int(asset_type_code))
        if key in allowed_meter_pairs:
            meter_guids.add(normalize_guid(global_id))

    return meter_guids


def trace_one_transformer(
    utility_network: str,
    starting_points_fc: str,
    domain_network: str,
    tier: str,
    trace_json_path: str,
    condition_barriers: Optional[List[List[object]]],
) -> None:
    kwargs = {
        "include_containers": "EXCLUDE_CONTAINERS",
        "include_content": "EXCLUDE_CONTENT",
        "include_structures": "EXCLUDE_STRUCTURES",
        "include_barriers": "EXCLUDE_BARRIERS",
        "validate_consistency": "DO_NOT_VALIDATE_CONSISTENCY",
        "traversability_scope": "BOTH_JUNCTIONS_AND_EDGES",
        "result_types": ["FEATURES"],
        "out_json_file": trace_json_path,
        "include_geometry": "EXCLUDE_GEOMETRY",
        "include_domain_descriptions": "EXCLUDE_DOMAIN_DESCRIPTIONS",
        "allow_indeterminate_flow": "TRACE_INDETERMINATE_FLOW",
    }
    if condition_barriers:
        kwargs["condition_barriers"] = condition_barriers

    arcpy.un.Trace(
        utility_network,
        "DOWNSTREAM",
        starting_points_fc,
        None,
        domain_network,
        tier,
        **kwargs,
    )


def write_csv(path: str, fieldnames: Sequence[str], rows: Sequence[Dict[str, object]]) -> None:
    ensure_directory(path)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create trace starting points from ElectricDevice transformers, run a "
            "downstream trace from each, and write transformer-to-secondary-meter pairs."
        )
    )
    parser.add_argument("--utility-network", required=True, help="Utility Network layer or service URL")
    parser.add_argument("--electric-device-class", required=True, help="Path to the ElectricDevice feature class")
    parser.add_argument("--domain-network", required=True, help="Traditional domain network name, for example ElectricDistribution")
    parser.add_argument("--tier", required=True, help="Tier name used for the downstream trace")
    parser.add_argument("--scratch-gdb", required=True, help="Scratch file geodatabase used for start points and temporary trace JSON")
    parser.add_argument(
        "--starting-terminal",
        default="Low Side",
        help="Transformer terminal used as the trace start. Use the terminal name or terminal ID. Default: Low Side",
    )
    parser.add_argument(
        "--transformer-type-names",
        default=",".join(TRANSFORMER_TYPE_NAMES),
        help="Comma-separated asset type descriptions for transformer starts",
    )
    parser.add_argument(
        "--meter-type-names",
        default=",".join(METER_TYPE_NAMES),
        help="Comma-separated asset type descriptions to keep from the trace result",
    )
    parser.add_argument(
        "--device-status-attribute",
        default="Device Status",
        help="Network attribute name used for the open-device barrier",
    )
    parser.add_argument(
        "--open-device-status-value",
        default=None,
        help="Optional status value that should act as a barrier, for example 1 for Open",
    )
    parser.add_argument(
        "--output-csv",
        default=os.path.join(os.getcwd(), "transformer_to_secondary_meter.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--failure-csv",
        default=os.path.join(os.getcwd(), "transformer_trace_failures.csv"),
        help="Output CSV path for trace failures",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    transformer_type_names = [value.strip() for value in args.transformer_type_names.split(",") if value.strip()]
    meter_type_names = [value.strip() for value in args.meter_type_names.split(",") if value.strip()]

    ensure_file_gdb(args.scratch_gdb)

    asset_group_field = find_field(args.electric_device_class, ("ASSETGROUP",))
    asset_type_field = find_field(args.electric_device_class, ("ASSETTYPE",))
    log(f"Using {asset_group_field}/{asset_type_field} from {args.electric_device_class}")

    transformer_pairs = get_asset_type_pairs(args.electric_device_class, transformer_type_names)
    meter_pairs = get_asset_type_pairs(args.electric_device_class, meter_type_names)
    meter_pair_keys = set(make_pair_lookup(meter_pairs).keys())

    transformers, _ = build_feature_lookup(
        args.electric_device_class,
        transformer_pairs,
        "Transformer",
    )
    _, meters_by_guid = build_feature_lookup(
        args.electric_device_class,
        meter_pairs,
        "Meter",
    )

    if not transformers:
        fail("No transformer features matched the requested transformer asset types.")
    if not meters_by_guid:
        fail("No meter features matched the requested secondary meter asset types.")

    log(f"Found {len(transformers)} transformers to trace.")
    log(f"Loaded {len(meters_by_guid)} candidate secondary meters for result matching.")

    starting_points_fc = os.path.join(args.scratch_gdb, "TraceStartingPoints")
    if arcpy.Exists(starting_points_fc):
        arcpy.management.Delete(starting_points_fc)

    condition_barriers = None
    if args.open_device_status_value is not None and str(args.open_device_status_value).strip() != "":
        condition_barriers = [
            [
                args.device_status_attribute,
                "IS_EQUAL_TO",
                "SPECIFIC_VALUE",
                str(args.open_device_status_value),
                "",
            ]
        ]

    temp_dir = tempfile.mkdtemp(prefix="trace_json_", dir=os.path.dirname(args.output_csv) or None)
    output_rows: List[Dict[str, object]] = []
    failure_rows: List[Dict[str, object]] = []

    for index, transformer in enumerate(transformers, start=1):
        transformer_guid = transformer["global_id"]
        transformer_oid = transformer["object_id"]
        trace_json = os.path.join(temp_dir, f"trace_{index}.json")

        log(
            f"[{index}/{len(transformers)}] tracing transformer OID {transformer_oid} "
            f"({transformer['asset_type_name']})"
        )

        try:
            create_single_starting_point(
                args.utility_network,
                args.electric_device_class,
                transformer_guid,
                args.starting_terminal,
                starting_points_fc,
            )
            trace_one_transformer(
                args.utility_network,
                starting_points_fc,
                args.domain_network,
                args.tier,
                trace_json,
                condition_barriers,
            )

            meter_guids = collect_meter_guids_from_trace_json(trace_json, meter_pair_keys)
            for meter_guid in sorted(meter_guids):
                meter = meters_by_guid.get(meter_guid)
                if not meter:
                    continue

                output_rows.append(
                    {
                        "transformer_globalid": transformer_guid,
                        "transformer_objectid": transformer_oid,
                        "transformer_asset_group": transformer["asset_group_name"],
                        "transformer_asset_type": transformer["asset_type_name"],
                        "meter_globalid": meter_guid,
                        "meter_objectid": meter["object_id"],
                        "meter_asset_group": meter["asset_group_name"],
                        "meter_asset_type": meter["asset_type_name"],
                    }
                )

        except Exception as exc:
            failure_rows.append(
                {
                    "transformer_globalid": transformer_guid,
                    "transformer_objectid": transformer_oid,
                    "transformer_asset_type": transformer["asset_type_name"],
                    "error": str(exc),
                }
            )
            warn(f"Trace failed for transformer OID {transformer_oid}: {exc}")

    write_csv(
        args.output_csv,
        (
            "transformer_globalid",
            "transformer_objectid",
            "transformer_asset_group",
            "transformer_asset_type",
            "meter_globalid",
            "meter_objectid",
            "meter_asset_group",
            "meter_asset_type",
        ),
        output_rows,
    )
    write_csv(
        args.failure_csv,
        (
            "transformer_globalid",
            "transformer_objectid",
            "transformer_asset_type",
            "error",
        ),
        failure_rows,
    )

    log(f"Wrote {len(output_rows)} transformer-to-meter rows to {args.output_csv}")
    log(f"Wrote {len(failure_rows)} failures to {args.failure_csv}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        fail(str(exc))
        sys.exit(1)
