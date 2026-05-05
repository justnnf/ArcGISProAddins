"""Nightly ArcPy utility-network trace from Service Location to feeding transformer.

This script is designed for a first-pass nightly batch process:
1. Filter the input service-location layer.
2. Build a temporary starting-points feature class.
3. Run one UPSTREAM trace per service location.
4. Parse the nearest transformer from the trace JSON.
5. Write one row per service location to an output table.

It supports both ArcGIS Pro script-tool parameters and command-line arguments.

Script tool parameter order
---------------------------
0  in_utility_network
1  in_service_location_layer
2  domain_network
3  tier
4  out_table
5  out_json_folder
6  scratch_gdb
7  transformer_nearest_assets
8  service_id_field
9  transformer_id_field
10 transformer_result_source
11 subnetwork_field
12 subnetwork_names
13 max_devices
14 extra_where_clause
15 trace_config_name
16 keep_trace_json
17 validate_consistency
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import arcpy


DEFAULT_START_POINTS_NAME = "temp_service_location_start_points"
OUTPUT_FIELDS = [
    ("RUN_ID", "TEXT", 64),
    ("RUN_TS", "DATE", None),
    ("SERVICE_GLOBALID", "TEXT", 64),
    ("SERVICE_ID", "TEXT", 255),
    ("SERVICE_OBJECTID", "LONG", None),
    ("SUBNETWORK_NAME", "TEXT", 255),
    ("TRANSFORMER_GLOBALID", "TEXT", 64),
    ("TRANSFORMER_ID", "TEXT", 255),
    ("TRANSFORMER_SOURCE", "TEXT", 255),
    ("TRANSFORMER_ASSETGROUP", "TEXT", 255),
    ("TRANSFORMER_ASSETTYPE", "TEXT", 255),
    ("TRACE_JSON", "TEXT", 512),
    ("STATUS", "TEXT", 50),
    ("MESSAGE", "TEXT", 2000),
]


@dataclass
class Config:
    in_utility_network: str
    in_service_location_layer: str
    domain_network: str
    tier: Optional[str]
    out_table: str
    out_json_folder: str
    scratch_gdb: str
    transformer_nearest_assets: List[str]
    service_id_field: Optional[str]
    transformer_id_field: Optional[str]
    transformer_result_source: Optional[str]
    subnetwork_field: Optional[str]
    subnetwork_names: List[str]
    max_devices: Optional[int]
    extra_where_clause: Optional[str]
    trace_config_name: Optional[str]
    keep_trace_json: bool
    validate_consistency: bool


def add_message(message: str) -> None:
    print(message)
    try:
        arcpy.AddMessage(message)
    except Exception:
        pass


def add_warning(message: str) -> None:
    print(f"WARNING: {message}")
    try:
        arcpy.AddWarning(message)
    except Exception:
        pass


def add_error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    try:
        arcpy.AddError(message)
    except Exception:
        pass


def parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n", ""}:
        return False
    return default


def split_multi_value(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return [part.strip() for part in str(text).split(";") if part.strip()]


def normalize_guid(value: object) -> Optional[str]:
    if value in (None, "", "None"):
        return None
    text = str(value).strip()
    if not text:
        return None
    if not text.startswith("{"):
        text = "{" + text.strip("{}") + "}"
    return text.upper()


def sanitize_filename(text: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text or "").strip())
    cleaned = cleaned.strip("._")
    return cleaned or fallback


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def ensure_folder(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ensure_file_gdb(path: str) -> str:
    path = os.path.abspath(path)
    if path.lower().endswith(".gdb"):
        gdb_path = path
    else:
        gdb_path = os.path.join(path, "scratch_trace_work.gdb")

    parent = os.path.dirname(gdb_path)
    ensure_folder(parent)

    if not arcpy.Exists(gdb_path):
        add_message(f"Creating scratch geodatabase: {gdb_path}")
        arcpy.management.CreateFileGDB(parent, os.path.basename(gdb_path))
    return gdb_path


def split_table_path(table_path: str) -> Tuple[str, str]:
    workspace = os.path.dirname(table_path)
    name = os.path.basename(table_path)
    if not workspace or not name:
        raise ValueError(f"Invalid output table path: {table_path}")
    return workspace, name


def list_field_names(dataset: str) -> Dict[str, str]:
    return {field.name.lower(): field.name for field in arcpy.ListFields(dataset)}


def find_field_case_insensitive(dataset: str, requested_name: Optional[str]) -> Optional[str]:
    if not requested_name:
        return None
    field_map = list_field_names(dataset)
    return field_map.get(requested_name.lower())


def get_globalid_field(dataset: str) -> str:
    desc = arcpy.Describe(dataset)
    globalid_field = getattr(desc, "globalIDFieldName", None)
    if globalid_field:
        return globalid_field

    field_map = list_field_names(dataset)
    for candidate in ("globalid", "global_id"):
        if candidate in field_map:
            return field_map[candidate]
    raise ValueError(f"No GlobalID field found on {dataset}")


def build_where_clause(
    dataset: str,
    subnetwork_field: Optional[str],
    subnetwork_names: Sequence[str],
    extra_where_clause: Optional[str],
) -> Optional[str]:
    clauses: List[str] = []

    if subnetwork_field and subnetwork_names:
        actual_field = find_field_case_insensitive(dataset, subnetwork_field)
        if not actual_field:
            raise ValueError(
                f"Subnetwork field '{subnetwork_field}' was not found on {dataset}"
            )
        delimited = arcpy.AddFieldDelimiters(dataset, actual_field)
        names_sql = ", ".join(sql_quote(name) for name in subnetwork_names)
        clauses.append(f"{delimited} IN ({names_sql})")

    if extra_where_clause:
        clauses.append(f"({extra_where_clause})")

    if not clauses:
        return None
    return " AND ".join(clauses)


def create_filtered_layer(source: str, where_clause: Optional[str]) -> str:
    layer_name = f"svc_loc_filtered_{datetime.now().strftime('%H%M%S%f')}"
    if where_clause:
        arcpy.management.MakeFeatureLayer(source, layer_name, where_clause)
    else:
        arcpy.management.MakeFeatureLayer(source, layer_name)
    return layer_name


def fetch_candidates(
    filtered_layer: str,
    service_id_field: Optional[str],
    subnetwork_field: Optional[str],
    max_devices: Optional[int],
) -> List[Dict[str, object]]:
    oid_field = arcpy.Describe(filtered_layer).OIDFieldName
    globalid_field = get_globalid_field(filtered_layer)
    actual_service_id_field = find_field_case_insensitive(filtered_layer, service_id_field)
    actual_subnetwork_field = find_field_case_insensitive(filtered_layer, subnetwork_field)

    fields = [oid_field, globalid_field]
    if actual_service_id_field:
        fields.append(actual_service_id_field)
    if actual_subnetwork_field and actual_subnetwork_field not in fields:
        fields.append(actual_subnetwork_field)

    candidates: List[Dict[str, object]] = []
    with arcpy.da.SearchCursor(filtered_layer, fields, sql_clause=(None, f"ORDER BY {oid_field}")) as cursor:
        for row in cursor:
            row_map = dict(zip(fields, row))
            oid_value = row_map[oid_field]
            globalid_value = normalize_guid(row_map[globalid_field])
            if not globalid_value:
                continue

            service_id_value = row_map.get(actual_service_id_field) if actual_service_id_field else None
            subnetwork_value = row_map.get(actual_subnetwork_field) if actual_subnetwork_field else None

            candidates.append(
                {
                    "oid": oid_value,
                    "globalid": globalid_value,
                    "service_id": None if service_id_value is None else str(service_id_value),
                    "subnetwork_name": None if subnetwork_value is None else str(subnetwork_value),
                }
            )

            if max_devices and len(candidates) >= max_devices:
                break

    return candidates


def ensure_output_table(table_path: str) -> None:
    workspace, table_name = split_table_path(table_path)
    if not arcpy.Exists(workspace):
        raise ValueError(f"Output workspace does not exist: {workspace}")

    if not arcpy.Exists(table_path):
        add_message(f"Creating output table: {table_path}")
        arcpy.management.CreateTable(workspace, table_name)

    existing_fields = list_field_names(table_path)
    for field_name, field_type, field_length in OUTPUT_FIELDS:
        if field_name.lower() in existing_fields:
            continue
        kwargs = {}
        if field_length:
            kwargs["field_length"] = field_length
        arcpy.management.AddField(table_path, field_name, field_type, **kwargs)


def reset_output_for_run(table_path: str, run_id: str) -> None:
    field_map = list_field_names(table_path)
    if "run_id" not in field_map:
        return
    where = f"{arcpy.AddFieldDelimiters(table_path, field_map['run_id'])} = {sql_quote(run_id)}"
    view_name = f"out_table_view_{datetime.now().strftime('%H%M%S%f')}"
    arcpy.management.MakeTableView(table_path, view_name, where)
    try:
        count = int(arcpy.management.GetCount(view_name)[0])
        if count:
            arcpy.management.DeleteRows(view_name)
    finally:
        arcpy.management.Delete(view_name)


def create_temp_start_points(
    in_utility_network: str,
    temp_start_points_fc: str,
    service_layer_name: str,
    seed_globalid: str,
) -> None:
    parent = os.path.dirname(temp_start_points_fc)
    if not arcpy.Exists(parent):
        raise ValueError(
            "Temporary start points must be created in an existing geodatabase. "
            f"Missing workspace: {parent}"
        )

    if arcpy.Exists(temp_start_points_fc):
        arcpy.management.Delete(temp_start_points_fc)

    add_message(f"Creating temporary start points feature class: {temp_start_points_fc}")
    arcpy.un.AddTraceLocations(
        in_utility_network,
        temp_start_points_fc,
        "DO_NOT_LOAD_SELECTED_FEATURES",
        "CLEAR_LOCATIONS",
        [[service_layer_name, seed_globalid, None, None]],
        "TRAVERSABILITY_BARRIER",
    )


def refresh_start_point(
    in_utility_network: str,
    temp_start_points_fc: str,
    service_layer_name: str,
    service_globalid: str,
) -> None:
    arcpy.un.AddTraceLocations(
        in_utility_network,
        temp_start_points_fc,
        "DO_NOT_LOAD_SELECTED_FEATURES",
        "CLEAR_LOCATIONS",
        [[service_layer_name, service_globalid, None, None]],
        "TRAVERSABILITY_BARRIER",
    )


def iter_feature_like_dicts(value: object) -> Iterator[dict]:
    if isinstance(value, dict):
        keys_lower = {key.lower() for key in value.keys()}
        if keys_lower.intersection(
            {
                "networksourceid",
                "networksourcename",
                "assetgroupname",
                "assettypename",
                "featureglobalid",
                "globalid",
                "objectid",
                "attributes",
            }
        ):
            yield value
        for nested in value.values():
            yield from iter_feature_like_dicts(nested)
    elif isinstance(value, list):
        for item in value:
            yield from iter_feature_like_dicts(item)


def get_case_insensitive(mapping: dict, key_name: str) -> object:
    key_lower = key_name.lower()
    for key, value in mapping.items():
        if str(key).lower() == key_lower:
            return value
    return None


def extract_transformer_result(
    trace_json_path: str,
    transformer_id_field: Optional[str],
    allowed_sources: Optional[Sequence[str]],
) -> Dict[str, Optional[str]]:
    with open(trace_json_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    normalized_sources = {
        str(source).strip().lower() for source in (allowed_sources or []) if str(source).strip()
    }

    for candidate in iter_feature_like_dicts(payload):
        attributes = get_case_insensitive(candidate, "attributes")
        if not isinstance(attributes, dict):
            attributes = {}

        source_name = (
            get_case_insensitive(candidate, "networkSourceName")
            or get_case_insensitive(candidate, "sourceName")
            or get_case_insensitive(candidate, "layerName")
            or get_case_insensitive(attributes, "sourceName")
        )
        asset_group = (
            get_case_insensitive(candidate, "assetGroupName")
            or get_case_insensitive(attributes, "assetGroupName")
        )
        asset_type = (
            get_case_insensitive(candidate, "assetTypeName")
            or get_case_insensitive(attributes, "assetTypeName")
        )
        transformer_globalid = (
            get_case_insensitive(candidate, "featureGlobalId")
            or get_case_insensitive(candidate, "globalId")
            or get_case_insensitive(attributes, "globalId")
            or get_case_insensitive(attributes, "featureGlobalId")
        )

        if normalized_sources:
            if not source_name or str(source_name).strip().lower() not in normalized_sources:
                continue

        transformer_id = None
        if transformer_id_field:
            transformer_id = get_case_insensitive(attributes, transformer_id_field)
            if transformer_id is None:
                transformer_id = get_case_insensitive(candidate, transformer_id_field)

        has_identity = any([source_name, asset_group, asset_type, transformer_globalid, transformer_id])
        if not has_identity:
            continue

        return {
            "transformer_globalid": normalize_guid(transformer_globalid),
            "transformer_id": None if transformer_id is None else str(transformer_id),
            "transformer_source": None if source_name is None else str(source_name),
            "transformer_assetgroup": None if asset_group is None else str(asset_group),
            "transformer_assettype": None if asset_type is None else str(asset_type),
        }

    return {
        "transformer_globalid": None,
        "transformer_id": None,
        "transformer_source": None,
        "transformer_assetgroup": None,
        "transformer_assettype": None,
    }


def build_trace_kwargs(config: Config, trace_json_path: str) -> Dict[str, object]:
    kwargs: Dict[str, object] = {
        "include_containers": "EXCLUDE_CONTAINERS",
        "include_content": "EXCLUDE_CONTENT",
        "include_structures": "EXCLUDE_STRUCTURES",
        "validate_consistency": (
            "VALIDATE_CONSISTENCY" if config.validate_consistency else "DO_NOT_VALIDATE_CONSISTENCY"
        ),
        "filter_nearest": "FILTER_BY_NEAREST",
        "nearest_count": 1,
        "nearest_assets": config.transformer_nearest_assets,
        "result_types": "FEATURES",
        "use_trace_config": (
            "USE_TRACE_CONFIGURATION" if config.trace_config_name else "DO_NOT_USE_TRACE_CONFIGURATION"
        ),
        "trace_config_name": config.trace_config_name,
        "out_json_file": trace_json_path,
        "run_async": "RUN_SYNCHRONOUSLY",
        "include_geometry": "EXCLUDE_GEOMETRY",
        "include_domain_descriptions": "INCLUDE_DOMAIN_DESCRIPTIONS",
    }

    if config.transformer_result_source and config.transformer_id_field:
        kwargs["result_fields"] = [[config.transformer_result_source, config.transformer_id_field]]

    if config.tier:
        kwargs["tier"] = config.tier

    return kwargs


def run_trace(
    config: Config,
    temp_start_points_fc: str,
    trace_json_path: str,
) -> Dict[str, Optional[str]]:
    kwargs = build_trace_kwargs(config, trace_json_path)
    arcpy.un.Trace(
        in_utility_network=config.in_utility_network,
        trace_type="UPSTREAM",
        starting_points=temp_start_points_fc,
        domain_network=config.domain_network,
        **kwargs,
    )

    allowed_sources = [config.transformer_result_source] if config.transformer_result_source else []
    return extract_transformer_result(
        trace_json_path=trace_json_path,
        transformer_id_field=config.transformer_id_field,
        allowed_sources=allowed_sources,
    )


def insert_output_row(
    out_table: str,
    run_id: str,
    run_ts: datetime,
    service_globalid: str,
    service_id: Optional[str],
    service_objectid: int,
    subnetwork_name: Optional[str],
    transformer: Dict[str, Optional[str]],
    trace_json_path: str,
    status: str,
    message: Optional[str],
) -> None:
    values = [
        run_id,
        run_ts,
        service_globalid,
        service_id,
        service_objectid,
        subnetwork_name,
        transformer.get("transformer_globalid"),
        transformer.get("transformer_id"),
        transformer.get("transformer_source"),
        transformer.get("transformer_assetgroup"),
        transformer.get("transformer_assettype"),
        trace_json_path,
        status,
        message,
    ]
    field_names = [field_name for field_name, _, _ in OUTPUT_FIELDS]
    with arcpy.da.InsertCursor(out_table, field_names) as cursor:
        cursor.insertRow(values)


def parse_cli_args(argv: Sequence[str]) -> Config:
    parser = argparse.ArgumentParser(
        description="Trace each Service Location upstream to the nearest transformer."
    )
    parser.add_argument("--utility-network", required=True, dest="in_utility_network")
    parser.add_argument("--service-locations", required=True, dest="in_service_location_layer")
    parser.add_argument("--domain-network", required=True, dest="domain_network")
    parser.add_argument("--tier", dest="tier")
    parser.add_argument("--out-table", required=True, dest="out_table")
    parser.add_argument("--out-json-folder", required=True, dest="out_json_folder")
    parser.add_argument("--scratch-gdb", required=True, dest="scratch_gdb")
    parser.add_argument(
        "--transformer-nearest-assets",
        required=True,
        dest="transformer_nearest_assets",
        help="Semicolon-delimited asset paths such as ElectricDistributionDevice/Transformer/Step Down",
    )
    parser.add_argument("--service-id-field", dest="service_id_field")
    parser.add_argument("--transformer-id-field", dest="transformer_id_field")
    parser.add_argument(
        "--transformer-result-source",
        dest="transformer_result_source",
        help="Feature source name used by the trace JSON for the transformer class.",
    )
    parser.add_argument("--subnetwork-field", dest="subnetwork_field")
    parser.add_argument("--subnetwork-names", dest="subnetwork_names")
    parser.add_argument("--max-devices", dest="max_devices", type=int)
    parser.add_argument("--where", dest="extra_where_clause")
    parser.add_argument("--trace-config-name", dest="trace_config_name")
    parser.add_argument("--keep-trace-json", action="store_true", dest="keep_trace_json")
    parser.add_argument(
        "--skip-validate-consistency",
        action="store_true",
        dest="skip_validate_consistency",
    )
    args = parser.parse_args(argv)

    return Config(
        in_utility_network=args.in_utility_network,
        in_service_location_layer=args.in_service_location_layer,
        domain_network=args.domain_network,
        tier=args.tier,
        out_table=args.out_table,
        out_json_folder=args.out_json_folder,
        scratch_gdb=args.scratch_gdb,
        transformer_nearest_assets=split_multi_value(args.transformer_nearest_assets),
        service_id_field=args.service_id_field,
        transformer_id_field=args.transformer_id_field,
        transformer_result_source=args.transformer_result_source,
        subnetwork_field=args.subnetwork_field,
        subnetwork_names=split_multi_value(args.subnetwork_names),
        max_devices=args.max_devices,
        extra_where_clause=args.extra_where_clause,
        trace_config_name=args.trace_config_name,
        keep_trace_json=args.keep_trace_json,
        validate_consistency=not args.skip_validate_consistency,
    )


def parse_script_tool_args() -> Config:
    get_text = arcpy.GetParameterAsText
    get_value = arcpy.GetParameter

    return Config(
        in_utility_network=get_text(0),
        in_service_location_layer=get_text(1),
        domain_network=get_text(2),
        tier=get_text(3) or None,
        out_table=get_text(4),
        out_json_folder=get_text(5),
        scratch_gdb=get_text(6),
        transformer_nearest_assets=split_multi_value(get_text(7)),
        service_id_field=get_text(8) or None,
        transformer_id_field=get_text(9) or None,
        transformer_result_source=get_text(10) or None,
        subnetwork_field=get_text(11) or None,
        subnetwork_names=split_multi_value(get_text(12)),
        max_devices=(int(get_text(13)) if get_text(13) else None),
        extra_where_clause=get_text(14) or None,
        trace_config_name=get_text(15) or None,
        keep_trace_json=parse_bool(get_value(16), default=False),
        validate_consistency=parse_bool(get_value(17), default=True),
    )


def resolve_config(argv: Sequence[str]) -> Config:
    if len(argv) > 1:
        return parse_cli_args(argv[1:])

    try:
        if arcpy.GetArgumentCount() > 0:
            return parse_script_tool_args()
    except Exception:
        pass

    raise ValueError(
        "No parameters were supplied. Use command-line arguments or configure script tool parameters."
    )


def validate_config(config: Config) -> None:
    if not config.transformer_nearest_assets:
        raise ValueError("At least one transformer nearest-asset path is required.")

    if config.transformer_id_field and not config.transformer_result_source:
        add_warning(
            "Transformer ID field was supplied without transformer result source. "
            "The output will still try to parse the field, but the JSON may not include it."
        )

    if config.subnetwork_names and not config.subnetwork_field:
        raise ValueError("A subnetwork field is required when subnetwork names are provided.")


def main(argv: Sequence[str]) -> int:
    config = resolve_config(argv)
    validate_config(config)

    run_ts = datetime.now()
    run_id = run_ts.strftime("%Y%m%d_%H%M%S")

    add_message(f"Run ID: {run_id}")
    add_message(f"Utility network: {config.in_utility_network}")
    add_message(f"Service locations: {config.in_service_location_layer}")

    ensure_folder(config.out_json_folder)
    config.scratch_gdb = ensure_file_gdb(config.scratch_gdb)
    ensure_output_table(config.out_table)
    reset_output_for_run(config.out_table, run_id)

    where_clause = build_where_clause(
        dataset=config.in_service_location_layer,
        subnetwork_field=config.subnetwork_field,
        subnetwork_names=config.subnetwork_names,
        extra_where_clause=config.extra_where_clause,
    )
    if where_clause:
        add_message(f"Service location filter: {where_clause}")

    filtered_layer = create_filtered_layer(config.in_service_location_layer, where_clause)
    try:
        candidates = fetch_candidates(
            filtered_layer=filtered_layer,
            service_id_field=config.service_id_field,
            subnetwork_field=config.subnetwork_field,
            max_devices=config.max_devices,
        )
        add_message(f"Candidate service locations: {len(candidates)}")
        if not candidates:
            add_warning("No candidate service locations matched the current filters.")
            return 0

        service_layer_name = arcpy.Describe(config.in_service_location_layer).baseName
        temp_start_points_fc = os.path.join(config.scratch_gdb, DEFAULT_START_POINTS_NAME)
        create_temp_start_points(
            in_utility_network=config.in_utility_network,
            temp_start_points_fc=temp_start_points_fc,
            service_layer_name=service_layer_name,
            seed_globalid=candidates[0]["globalid"],
        )

        total = len(candidates)
        for index, candidate in enumerate(candidates, start=1):
            service_globalid = str(candidate["globalid"])
            service_id = candidate["service_id"] or str(candidate["oid"])
            json_name = f"{run_id}_{index:06d}_{sanitize_filename(service_id, str(candidate['oid']))}.json"
            trace_json_path = os.path.join(config.out_json_folder, json_name)

            add_message(
                f"[{index}/{total}] Tracing service location "
                f"{service_id} ({service_globalid})"
            )

            try:
                refresh_start_point(
                    in_utility_network=config.in_utility_network,
                    temp_start_points_fc=temp_start_points_fc,
                    service_layer_name=service_layer_name,
                    service_globalid=service_globalid,
                )
                transformer = run_trace(
                    config=config,
                    temp_start_points_fc=temp_start_points_fc,
                    trace_json_path=trace_json_path,
                )

                if transformer["transformer_globalid"] or transformer["transformer_id"]:
                    status = "OK"
                    message = None
                else:
                    status = "NOT_FOUND"
                    message = "Trace completed but no transformer feature was parsed from the JSON results."

            except Exception as exc:
                transformer = {
                    "transformer_globalid": None,
                    "transformer_id": None,
                    "transformer_source": None,
                    "transformer_assetgroup": None,
                    "transformer_assettype": None,
                }
                status = "ERROR"
                message = f"{type(exc).__name__}: {exc}"
                add_warning(f"Trace failed for service location {service_id}: {message}")

            insert_output_row(
                out_table=config.out_table,
                run_id=run_id,
                run_ts=run_ts,
                service_globalid=service_globalid,
                service_id=service_id,
                service_objectid=int(candidate["oid"]),
                subnetwork_name=candidate["subnetwork_name"],
                transformer=transformer,
                trace_json_path=trace_json_path,
                status=status,
                message=message,
            )

            if not config.keep_trace_json and os.path.exists(trace_json_path) and status != "ERROR":
                os.remove(trace_json_path)

        add_message("Nightly service-location trace run completed.")
        return 0

    finally:
        try:
            arcpy.management.Delete(filtered_layer)
        except Exception:
            pass


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except Exception as exc:
        add_error(str(exc))
        add_error(traceback.format_exc())
        sys.exit(1)
