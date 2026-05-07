import csv
import datetime
import json
import os
import re
import traceback
from dataclasses import dataclass

import arcpy


TARGET_ARCGIS_PRO = "3.3"
TARGET_CIM_VERSION = "V3"
FIELD_SETTINGS_COLUMNS = [
    "Map Name",
    "Class Name",
    "Layer Name",
    "Subtype Value",
    "Field Name",
    "Field Order",
    "Visible",
    "Read Only",
    "Highlight",
    "Field Alias",
    "Map Member URI",
    "Long Name",
    "Member Kind",
]
LAYER_SCALE_COLUMNS = [
    "Map Name",
    "Layer Type",
    "Group Layer Name",
    "Layer Name",
    "Long Name",
    "Layer URI",
    "Layer Min Scale",
    "Layer Max Scale",
    "Label Min Scale",
    "Label Max Scale",
]
DISPLAY_EXPRESSION_POPUP_NAME = "display_expression"
MULTILINE_TEXT_CONTROL = "{E5456E51-0C41-4797-9EE4-5269820C6F0E}"
MULTIVALUE_CHECKBOX_SELECT_ALL_CONTROL = "{38C34610-C7F7-11D5-A693-0008C711C8C1}"
UTILITY_NETWORK_INPUT_DATATYPES = ["GPUtilityNetworkLayer", "DEUtilityNetwork"]
UN_EXPORT_OPTIONS = [
    "Utility Network Summary",
    "Association And System Sources",
    "Domain Networks",
    "Tier Groups",
    "Tiers",
    "Edge Sources",
    "Junction Sources",
    "Domains",
    "Domain Assignments",
    "Asset Groups",
    "Asset Types",
    "Network Category Assignments",
    "Network Attributes",
    "Network Attribute Assignments",
    "Categories",
    "Terminal Configurations",
    "Terminal Assignments",
    "Terminals",
    "Valid Configuration Paths",
]


class ToolboxError(RuntimeError):
    pass


@dataclass
class MapMemberRecord:
    map_name: str
    member: object
    definition: object
    member_kind: str
    name: str
    long_name: str
    class_name: str
    uri: str
    subtype_value: str


def _sanitize_name(value):
    value = value or "CurrentMap"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "CurrentMap"


def _get_current_project():
    try:
        return arcpy.mp.ArcGISProject("CURRENT")
    except Exception as exc:
        raise ToolboxError(
            "This toolbox must be run from inside ArcGIS Pro. The CURRENT project is not available."
        ) from exc


def _get_active_map():
    aprx = _get_current_project()
    active_map = aprx.activeMap
    if active_map is None:
        raise ToolboxError("Open a map view before running this tool.")
    return aprx, active_map


def _list_project_maps():
    aprx = _get_current_project()
    return aprx, aprx.listMaps()


def _get_map_names():
    _, maps = _list_project_maps()
    return [map_obj.name for map_obj in maps]


def _parse_multivalue_text(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item.strip().strip("'").strip('"') for item in text.split(";") if item.strip()]


def _resolve_map(map_name=None):
    aprx = _get_current_project()
    maps = aprx.listMaps()
    if not maps:
        raise ToolboxError("The current ArcGIS Pro project does not contain any maps.")

    if map_name:
        for map_obj in maps:
            if map_obj.name == map_name:
                return aprx, map_obj
        raise ToolboxError("Map '{0}' was not found in the current project.".format(map_name))

    active_map = aprx.activeMap
    if active_map is not None:
        return aprx, active_map

    return aprx, maps[0]


def _get_default_output_file(suffix, extension, map_name=None):
    aprx, active_map = _resolve_map(map_name)
    base_folder = aprx.homeFolder or arcpy.env.scratchFolder or os.getcwd()
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = "{0}_{1}_{2}.{3}".format(
        timestamp,
        _sanitize_name(active_map.name),
        suffix,
        extension,
    )
    return os.path.join(base_folder, file_name)


def _ensure_parent_folder(file_path):
    parent = os.path.dirname(os.path.abspath(file_path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)


def _is_enum_like(value):
    return hasattr(value, "name") and hasattr(value, "value") and not hasattr(value, "__dict__")


def _cim_to_jsonable(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_cim_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _cim_to_jsonable(item) for key, item in value.items()}
    if _is_enum_like(value):
        return {"__enum_value__": value.value, "__enum_name__": value.name}
    if hasattr(value, "__dict__"):
        payload = {"__cim_type__": type(value).__name__}
        for key, item in value.__dict__.items():
            payload[key] = _cim_to_jsonable(item)
        return payload
    return value


def _jsonable_to_cim(value, template=None):
    if isinstance(value, list):
        template_items = template if isinstance(template, list) else []
        rebuilt = []
        for index, item in enumerate(value):
            child_template = template_items[index] if index < len(template_items) else None
            rebuilt.append(_jsonable_to_cim(item, child_template))
        return rebuilt
    if isinstance(value, dict):
        if "__enum_value__" in value:
            if template is not None and _is_enum_like(template):
                try:
                    return type(template)(value["__enum_value__"])
                except Exception:
                    return value["__enum_value__"]
            return value["__enum_value__"]
        if "__cim_type__" in value:
            cim_object = arcpy.cim.CreateCIMObjectFromClassName(value["__cim_type__"], TARGET_CIM_VERSION)
            for key, item in value.items():
                if key == "__cim_type__":
                    continue
                current_value = getattr(cim_object, key, None)
                setattr(cim_object, key, _jsonable_to_cim(item, current_value))
            return cim_object
        return {key: _jsonable_to_cim(item) for key, item in value.items()}
    if template is not None and _is_enum_like(template):
        try:
            return type(template)(value)
        except Exception:
            return value
    return value


def _normalize_dataset_name(raw_value):
    if not raw_value:
        return ""
    dataset_name = str(raw_value).replace("\\", "/").rstrip("/")
    if "/" in dataset_name:
        dataset_name = dataset_name.split("/")[-1]
    if "." in dataset_name and not dataset_name.lower().endswith(".csv"):
        dataset_name = dataset_name.split(".")[-1]
    return dataset_name


def _derive_class_name(member, definition):
    candidates = []

    feature_table = getattr(definition, "featureTable", None)
    if feature_table is not None:
        data_connection = getattr(feature_table, "dataConnection", None)
        dataset = getattr(data_connection, "dataset", "")
        if dataset:
            candidates.append(dataset)

    data_connection = getattr(definition, "dataConnection", None)
    dataset = getattr(data_connection, "dataset", "")
    if dataset:
        candidates.append(dataset)

    data_source = getattr(member, "dataSource", "")
    if data_source:
        candidates.append(data_source)

    candidates.append(getattr(member, "name", ""))

    for candidate in candidates:
        normalized = _normalize_dataset_name(candidate)
        if normalized:
            return normalized

    return getattr(member, "name", "Unknown")


def _safe_long_name(member, fallback_name):
    long_name = getattr(member, "longName", "") or ""
    return long_name or fallback_name


def _walk_layers(container):
    for layer in container.listLayers():
        yield layer
        list_layers = getattr(layer, "listLayers", None)
        if callable(list_layers):
            for child in _walk_layers(layer):
                yield child


def _get_all_layers(map_name=None):
    _, active_map = _resolve_map(map_name)
    return list(_walk_layers(active_map))


def _get_group_layer_name(layer):
    long_name = getattr(layer, "longName", "") or ""
    if "\\" not in long_name:
        return ""
    parts = [part for part in long_name.split("\\") if part]
    if len(parts) <= 1:
        return ""
    return parts[-2]


def _get_layer_uri(layer, definition=None):
    if definition is None:
        definition = layer.getDefinition(TARGET_CIM_VERSION)
    return getattr(layer, "URI", "") or getattr(definition, "uRI", "")


def _get_layer_type_description(layer):
    if getattr(layer, "isGroupLayer", False):
        return "Group Layer"
    if getattr(layer, "isFeatureLayer", False):
        return "Feature Layer"
    if getattr(layer, "isRasterLayer", False):
        return "Raster Layer"
    if getattr(layer, "isBasemapLayer", False):
        return "Basemap Layer"
    if getattr(layer, "isWebLayer", False):
        return "Web Layer"
    return layer.__class__.__name__


def _supports_property(layer, property_name):
    supports = getattr(layer, "supports", None)
    if callable(supports):
        try:
            return bool(supports(property_name))
        except Exception:
            return False
    return False


def _scale_to_text(value):
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return "None"
    if numeric_value == 0:
        return "None"
    if numeric_value.is_integer():
        return str(int(numeric_value))
    return str(numeric_value)


def _parse_scale_value(value, keep_existing=False):
    text = str(value or "").strip()
    if not text:
        return None if keep_existing else 0.0
    if text.lower() in ("none", "<none>", "null"):
        return 0.0
    try:
        return float(text.replace(",", ""))
    except ValueError as exc:
        raise ToolboxError("Scale value '{0}' is not valid.".format(value)) from exc


def _get_label_scale_range(layer):
    if not _supports_property(layer, "SHOWLABELS"):
        return ("N/A", "N/A")

    try:
        label_classes = layer.listLabelClasses()
    except Exception:
        return ("N/A", "N/A")

    if not label_classes:
        return ("None", "None")

    minimum_values = []
    maximum_values = []
    for label_class in label_classes:
        label_definition = label_class.getDefinition(TARGET_CIM_VERSION)
        minimum_values.append(getattr(label_definition, "minimumScale", 0) or 0)
        maximum_values.append(getattr(label_definition, "maximumScale", 0) or 0)

    if len(set(minimum_values)) == 1 and len(set(maximum_values)) == 1:
        return (_scale_to_text(minimum_values[0]), _scale_to_text(maximum_values[0]))
    return ("Multiple", "Multiple")


def _walk_tables(container):
    list_tables = getattr(container, "listTables", None)
    if callable(list_tables):
        for table in container.listTables():
            yield table
            for child in _walk_tables(table):
                yield child

    list_layers = getattr(container, "listLayers", None)
    if callable(list_layers):
        for layer in container.listLayers():
            for table in _walk_tables(layer):
                yield table


def _build_feature_layer_records(active_map):
    records = []
    seen_uris = set()
    for layer in _walk_layers(active_map):
        if not getattr(layer, "isFeatureLayer", False):
            continue
        if getattr(layer, "isBroken", False):
            arcpy.AddWarning("Skipping broken layer: {0}".format(layer.name))
            continue

        definition = layer.getDefinition(TARGET_CIM_VERSION)
        feature_table = getattr(definition, "featureTable", None)
        subtype_value = ""
        if feature_table is not None and getattr(feature_table, "useSubtypeValue", False):
            subtype_value = str(getattr(feature_table, "subtypeValue", ""))

        uri = getattr(layer, "URI", "") or getattr(definition, "uRI", "")
        if uri in seen_uris:
            continue
        seen_uris.add(uri)

        records.append(
            MapMemberRecord(
                map_name=active_map.name,
                member=layer,
                definition=definition,
                member_kind="Feature Layer",
                name=layer.name,
                long_name=_safe_long_name(layer, layer.name),
                class_name=_derive_class_name(layer, definition),
                uri=uri,
                subtype_value=subtype_value,
            )
        )
    return records


def _build_table_records(active_map):
    records = []
    seen_uris = set()
    for table in _walk_tables(active_map):
        if getattr(table, "isBroken", False):
            arcpy.AddWarning("Skipping broken table: {0}".format(table.name))
            continue

        definition = table.getDefinition(TARGET_CIM_VERSION)
        subtype_value = ""
        if getattr(definition, "useSubtypeValue", False):
            subtype_value = str(getattr(definition, "subtypeValue", ""))

        uri = getattr(table, "URI", "") or getattr(definition, "uRI", "")
        if uri in seen_uris:
            continue
        seen_uris.add(uri)

        records.append(
            MapMemberRecord(
                map_name=active_map.name,
                member=table,
                definition=definition,
                member_kind="Standalone Table",
                name=table.name,
                long_name=table.name,
                class_name=_derive_class_name(table, definition),
                uri=uri,
                subtype_value=subtype_value,
            )
        )
    return records


def _get_all_supported_members(map_name=None):
    _, active_map = _resolve_map(map_name)
    return _build_feature_layer_records(active_map) + _build_table_records(active_map)


def _get_selectable_member_names(map_name=None):
    return [record.long_name for record in _get_all_supported_members(map_name)]


def _create_default_field_description(field):
    field_description = arcpy.cim.CreateCIMObjectFromClassName("CIMFieldDescription", TARGET_CIM_VERSION)
    field_description.fieldName = field.name
    field_description.alias = getattr(field, "aliasName", None) or field.name
    field_description.highlight = False
    field_description.visible = True
    field_description.readOnly = not bool(getattr(field, "editable", False))
    field_description.searchable = False
    return field_description


def _get_member_fields(record):
    data_source = getattr(record.member, "dataSource", None)
    if data_source:
        try:
            described = arcpy.Describe(data_source)
            if hasattr(described, "fields"):
                return list(described.fields)
        except Exception:
            pass

    try:
        described = arcpy.Describe(record.member)
        if hasattr(described, "fields"):
            return list(described.fields)
    except Exception:
        pass

    field_descriptions = []
    if record.member_kind == "Feature Layer":
        field_descriptions = list(getattr(record.definition.featureTable, "fieldDescriptions", []) or [])
    else:
        field_descriptions = list(getattr(record.definition, "fieldDescriptions", []) or [])

    fallback_fields = []
    for field_description in field_descriptions:
        field_name = getattr(field_description, "fieldName", "")
        if field_name:
            fallback_fields.append(
                type(
                    "FieldStub",
                    (),
                    {
                        "name": field_name,
                        "aliasName": getattr(field_description, "alias", "") or field_name,
                        "editable": not bool(getattr(field_description, "readOnly", False)),
                        "type": "String",
                    },
                )()
            )
    return fallback_fields


def _get_complete_field_descriptions(record):
    if record.member_kind == "Feature Layer":
        existing = list(getattr(record.definition.featureTable, "fieldDescriptions", []) or [])
    else:
        existing = list(getattr(record.definition, "fieldDescriptions", []) or [])

    existing_lookup = {fd.fieldName: fd for fd in existing if getattr(fd, "fieldName", None)}
    complete = []

    for field in _get_member_fields(record):
        field_description = existing_lookup.get(field.name)
        if field_description is None:
            field_description = _create_default_field_description(field)
        complete.append(field_description)

    complete_names = {fd.fieldName for fd in complete}
    for field_description in existing:
        if getattr(field_description, "fieldName", None) not in complete_names:
            complete.append(field_description)

    return complete


def _set_field_descriptions(record, field_descriptions):
    if record.member_kind == "Feature Layer":
        record.definition.featureTable.fieldDescriptions = field_descriptions
    else:
        record.definition.fieldDescriptions = field_descriptions


def _get_display_field(record):
    if record.member_kind == "Feature Layer":
        return getattr(record.definition.featureTable, "displayField", "")
    return getattr(record.definition, "displayField", "")


def _get_popup_info(record):
    return getattr(record.definition, "popupInfo", None)


def _set_popup_info(record, popup_info):
    record.definition.popupInfo = popup_info


def _get_display_expression_info(record):
    if record.member_kind == "Feature Layer":
        return getattr(record.definition.featureTable, "displayExpressionInfo", None)
    return getattr(record.definition, "displayExpressionInfo", None)


def _set_display_expression_info(record, expression_info):
    if record.member_kind == "Feature Layer":
        record.definition.featureTable.displayExpressionInfo = expression_info
    else:
        record.definition.displayExpressionInfo = expression_info


def _save_record_definition(record):
    record.member.setDefinition(record.definition)


def _record_to_field_rows(record):
    rows = []
    for field_order, field_description in enumerate(_get_complete_field_descriptions(record), start=1):
        rows.append(
            {
                "Map Name": record.map_name,
                "Class Name": record.class_name,
                "Layer Name": record.name,
                "Subtype Value": record.subtype_value,
                "Field Name": field_description.fieldName,
                "Field Order": field_order,
                "Visible": bool(getattr(field_description, "visible", True)),
                "Read Only": bool(getattr(field_description, "readOnly", False)),
                "Highlight": bool(getattr(field_description, "highlight", False)),
                "Field Alias": getattr(field_description, "alias", "") or "",
                "Map Member URI": record.uri,
                "Long Name": record.long_name,
                "Member Kind": record.member_kind,
            }
        )
    return rows


def _write_field_settings_csv(output_file, map_name=None):
    rows = []
    for record in _get_all_supported_members(map_name):
        rows.extend(_record_to_field_rows(record))

    _ensure_parent_folder(output_file)
    with open(output_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELD_SETTINGS_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    arcpy.AddMessage("Exported field settings for {0} map members.".format(len({row['Map Member URI'] for row in rows})))


def _normalize_header(value):
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _read_csv_records(input_file):
    with open(input_file, "r", newline="", encoding="utf-8-sig") as handle:
        lines = handle.readlines()

    header_index = None
    for index, line in enumerate(lines):
        first_cell = line.split(",", 1)[0].strip().strip('"')
        if first_cell in ("Class Name", "Map Name"):
            header_index = index
            break

    if header_index is None:
        raise ToolboxError("Could not find a recognized header row in {0}.".format(input_file))

    reader = csv.DictReader(lines[header_index:])
    return [row for row in reader if any((value or "").strip() for value in row.values())]


def _get_row_value(row, *names, default=""):
    normalized = {_normalize_header(key): key for key in row.keys()}
    for name in names:
        key = normalized.get(_normalize_header(name))
        if key is not None:
            return row.get(key, default)
    return default


def _parse_bool(value, default=False):
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "y", "t"):
        return True
    if text in ("false", "0", "no", "n", "f"):
        return False
    return default


def _build_member_lookup(map_name=None):
    records = _get_all_supported_members(map_name)
    by_uri = {}
    by_fallback = {}

    for record in records:
        if record.uri:
            by_uri[record.uri] = record

        fallback_key = (
            record.member_kind.lower(),
            record.class_name.lower(),
            record.name.lower(),
            record.subtype_value.lower(),
            record.long_name.lower(),
        )
        by_fallback[fallback_key] = record

        short_key = (
            record.member_kind.lower(),
            record.class_name.lower(),
            record.name.lower(),
            record.subtype_value.lower(),
            "",
        )
        by_fallback.setdefault(short_key, record)

    return by_uri, by_fallback


def _group_field_rows(rows):
    grouped = {}
    for row in rows:
        uri = _get_row_value(row, "Map Member URI")
        member_kind = _get_row_value(row, "Member Kind", default="Feature Layer").strip() or "Feature Layer"
        class_name = _get_row_value(row, "Class Name").strip()
        layer_name = _get_row_value(row, "Layer Name").strip()
        subtype_value = _get_row_value(row, "Subtype Value").strip()
        long_name = _get_row_value(row, "Long Name").strip()
        key = uri or "|".join([member_kind, class_name, layer_name, subtype_value, long_name])
        grouped.setdefault(key, []).append(row)
    return grouped


def _resolve_record(rows, by_uri, by_fallback):
    sample = rows[0]
    uri = _get_row_value(sample, "Map Member URI").strip()
    if uri and uri in by_uri:
        return by_uri[uri]

    fallback_key = (
        (_get_row_value(sample, "Member Kind", default="Feature Layer").strip() or "Feature Layer").lower(),
        _get_row_value(sample, "Class Name").strip().lower(),
        _get_row_value(sample, "Layer Name").strip().lower(),
        _get_row_value(sample, "Subtype Value").strip().lower(),
        _get_row_value(sample, "Long Name").strip().lower(),
    )
    if fallback_key in by_fallback:
        return by_fallback[fallback_key]

    short_key = fallback_key[:-1] + ("",)
    return by_fallback.get(short_key)


def _apply_field_settings_csv(input_file, map_name=None):
    rows = _read_csv_records(input_file)
    by_uri, by_fallback = _build_member_lookup(map_name)

    updated_count = 0
    skipped = []
    for _, grouped_rows in _group_field_rows(rows).items():
        record = _resolve_record(grouped_rows, by_uri, by_fallback)
        if record is None:
            sample = grouped_rows[0]
            skipped.append(
                "{0} / {1}".format(
                    _get_row_value(sample, "Class Name"),
                    _get_row_value(sample, "Layer Name"),
                )
            )
            continue

        current_field_descriptions = _get_complete_field_descriptions(record)
        current_lookup = {fd.fieldName: fd for fd in current_field_descriptions}
        ordered_descriptions = []
        consumed_names = set()

        def _sort_key(row):
            try:
                return int(_get_row_value(row, "Field Order", default="0") or "0")
            except ValueError:
                return 0

        for row in sorted(grouped_rows, key=_sort_key):
            field_name = _get_row_value(row, "Field Name").strip()
            field_description = current_lookup.get(field_name)
            if field_description is None:
                arcpy.AddWarning(
                    "Field '{0}' was not found on {1} and was skipped.".format(field_name, record.long_name)
                )
                continue

            field_description.visible = _parse_bool(_get_row_value(row, "Visible"), default=True)
            field_description.readOnly = _parse_bool(_get_row_value(row, "Read Only", "Read-Only"), default=False)
            field_description.highlight = _parse_bool(_get_row_value(row, "Highlight"), default=False)
            field_description.alias = _get_row_value(row, "Field Alias") or field_name

            ordered_descriptions.append(field_description)
            consumed_names.add(field_name)

        for field_description in current_field_descriptions:
            if field_description.fieldName not in consumed_names:
                ordered_descriptions.append(field_description)

        _set_field_descriptions(record, ordered_descriptions)
        _save_record_definition(record)
        updated_count += 1

    arcpy.AddMessage("Updated field settings for {0} map members.".format(updated_count))
    if skipped:
        arcpy.AddWarning(
            "Could not find {0} map members from the CSV: {1}".format(
                len(skipped),
                "; ".join(skipped[:10]),
            )
        )


def _build_layer_scale_row(map_name, layer, definition):
    label_min_scale, label_max_scale = _get_label_scale_range(layer)
    return {
        "Map Name": map_name,
        "Layer Type": _get_layer_type_description(layer),
        "Group Layer Name": _get_group_layer_name(layer),
        "Layer Name": layer.name,
        "Long Name": getattr(layer, "longName", "") or layer.name,
        "Layer URI": _get_layer_uri(layer, definition),
        "Layer Min Scale": _scale_to_text(getattr(layer, "minThreshold", 0)),
        "Layer Max Scale": _scale_to_text(getattr(layer, "maxThreshold", 0)),
        "Label Min Scale": label_min_scale,
        "Label Max Scale": label_max_scale,
    }


def _write_layer_scales_csv(output_file, map_name=None):
    _, active_map = _resolve_map(map_name)
    rows = []
    for layer in _get_all_layers(active_map.name):
        if getattr(layer, "isBroken", False):
            arcpy.AddWarning("Skipping broken layer: {0}".format(layer.name))
            continue
        definition = layer.getDefinition(TARGET_CIM_VERSION)
        rows.append(_build_layer_scale_row(active_map.name, layer, definition))

    _ensure_parent_folder(output_file)
    with open(output_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=LAYER_SCALE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    arcpy.AddMessage("Exported layer scales for {0} layers.".format(len(rows)))


def _build_layer_lookup(map_name=None):
    by_uri = {}
    by_long_name = {}
    for layer in _get_all_layers(map_name):
        if getattr(layer, "isBroken", False):
            continue
        definition = layer.getDefinition(TARGET_CIM_VERSION)
        uri = _get_layer_uri(layer, definition)
        if uri:
            by_uri[uri] = layer
        long_name = (getattr(layer, "longName", "") or layer.name).lower()
        by_long_name[long_name] = layer
    return by_uri, by_long_name


def _apply_layer_scales_csv(input_file, map_name=None):
    rows = _read_csv_records(input_file)
    by_uri, by_long_name = _build_layer_lookup(map_name)
    updated_count = 0
    skipped = []

    for row in rows:
        uri = _get_row_value(row, "Layer URI").strip()
        layer = by_uri.get(uri)
        if layer is None:
            long_name = (_get_row_value(row, "Long Name") or _get_row_value(row, "Layer Name")).strip().lower()
            layer = by_long_name.get(long_name)

        if layer is None:
            skipped.append(_get_row_value(row, "Long Name") or _get_row_value(row, "Layer Name"))
            continue

        min_scale = _parse_scale_value(_get_row_value(row, "Layer Min Scale"), keep_existing=True)
        max_scale = _parse_scale_value(_get_row_value(row, "Layer Max Scale"), keep_existing=True)

        if min_scale is not None and _supports_property(layer, "MINTHRESHOLD"):
            layer.minThreshold = min_scale
        if max_scale is not None and _supports_property(layer, "MAXTHRESHOLD"):
            layer.maxThreshold = max_scale

        label_min_text = _get_row_value(row, "Label Min Scale")
        label_max_text = _get_row_value(row, "Label Max Scale")
        if _supports_property(layer, "SHOWLABELS") and (str(label_min_text).strip() or str(label_max_text).strip()):
            if label_min_text not in ("Multiple", "multiple") and label_max_text not in ("Multiple", "multiple"):
                label_min_scale = _parse_scale_value(label_min_text, keep_existing=True)
                label_max_scale = _parse_scale_value(label_max_text, keep_existing=True)
                for label_class in layer.listLabelClasses():
                    label_definition = label_class.getDefinition(TARGET_CIM_VERSION)
                    if label_min_scale is not None:
                        label_definition.minimumScale = label_min_scale
                    if label_max_scale is not None:
                        label_definition.maximumScale = label_max_scale
                    label_class.setDefinition(label_definition)

        updated_count += 1

    arcpy.AddMessage("Updated scale settings for {0} layers.".format(updated_count))
    if skipped:
        arcpy.AddWarning(
            "Could not find {0} layers from the CSV: {1}".format(
                len(skipped),
                "; ".join(skipped[:10]),
            )
        )


def _normalize_output_name(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip()).strip("_") or "output"


def _write_rows_to_csv(output_file, rows, fieldnames=None):
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

    _ensure_parent_folder(output_file)
    with open(output_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _safe_getattr(value, *names, default=None):
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _stringify_value(value):
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return "; ".join(_stringify_value(item) for item in value)
    return str(value)


def _discover_utility_network_paths(map_name=None):
    search_roots = set()
    for record in _get_all_supported_members(map_name):
        data_source = getattr(record.member, "dataSource", None)
        if not data_source:
            continue

        try:
            desc = arcpy.Describe(data_source)
        except Exception:
            continue

        for candidate in [data_source, getattr(desc, "path", None)]:
            if candidate:
                search_roots.add(candidate)

    utility_network_paths = []
    seen = set()
    for root in search_roots:
        try:
            for dirpath, dirnames, filenames in arcpy.da.Walk(root, datatype="UtilityNetwork"):
                for filename in filenames:
                    path = _normalize_utility_network_input(os.path.join(dirpath, filename))
                    if not path:
                        continue
                    if path not in seen:
                        seen.add(path)
                        utility_network_paths.append(path)
        except Exception:
            continue

    return sorted(utility_network_paths)


def _normalize_utility_network_input(utility_network_input):
    if not utility_network_input:
        return ""

    try:
        desc = arcpy.Describe(utility_network_input)
    except Exception:
        return utility_network_input

    if getattr(desc, "dataType", "") == "UtilityNetwork":
        direct_catalog_path = _safe_getattr(desc, "catalogPath", "catalogpath")
        return direct_catalog_path or utility_network_input

    data_element = getattr(desc, "dataElement", None)
    if data_element is not None and getattr(data_element, "dataType", "") == "UtilityNetwork":
        catalog_path = _safe_getattr(data_element, "catalogPath", "catalogpath")
        return catalog_path or utility_network_input

    return ""


def _resolve_utility_network_path(map_name=None, utility_network_path=None):
    if utility_network_path:
        normalized_path = _normalize_utility_network_input(utility_network_path)
        if normalized_path:
            return normalized_path
        raise ToolboxError(
            "The Utility Network parameter must reference a utility network layer or a utility network dataset."
        )

    candidates = _discover_utility_network_paths(map_name)
    if not candidates:
        raise ToolboxError(
            "No utility network dataset could be discovered from the selected map. "
            "Use a map with utility network layers or provide a utility network dataset path."
        )
    if len(candidates) > 1:
        raise ToolboxError(
            "Multiple utility network datasets were discovered. Select one from the Utility Network parameter."
        )
    return candidates[0]


def _resolve_utility_network_context(utility_network_path):
    utility_network_desc = arcpy.Describe(utility_network_path)
    feature_dataset_path = getattr(utility_network_desc, "path", None) or ""

    workspace_path = feature_dataset_path
    seen = set()
    while workspace_path and workspace_path not in seen:
        seen.add(workspace_path)
        try:
            desc = arcpy.Describe(workspace_path)
        except Exception:
            break
        if getattr(desc, "dataType", "") == "Workspace":
            return utility_network_desc, feature_dataset_path, workspace_path
        next_path = getattr(desc, "path", None)
        if not next_path or next_path == workspace_path:
            break
        workspace_path = next_path

    return utility_network_desc, feature_dataset_path, feature_dataset_path


def _utility_network_output_file(output_folder, utility_network_path, suffix):
    utility_network_name = os.path.basename(utility_network_path.rstrip("\\/"))
    filename = "{0}_{1}.csv".format(_normalize_output_name(utility_network_name), _normalize_output_name(suffix))
    return os.path.join(output_folder, filename)


def _normalize_to_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [item for item in value if _stringify_value(item)]
    text = _stringify_value(value)
    if not text:
        return []
    if ";" in text:
        return [item.strip() for item in text.split(";") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def _lookup_domain_description(domain, code):
    if domain is None or code in (None, ""):
        return ""
    coded_values = getattr(domain, "codedValues", None)
    if not isinstance(coded_values, dict):
        return ""
    if code in coded_values:
        return _stringify_value(coded_values[code])
    for key, value in coded_values.items():
        if _stringify_value(key) == _stringify_value(code):
            return _stringify_value(value)
    return ""


def _iter_workspace_datasets(workspace_path):
    seen = set()
    try:
        for dirpath, dirnames, filenames in arcpy.da.Walk(workspace_path, datatype=["FeatureClass", "Table"]):
            for filename in filenames:
                dataset_path = os.path.join(dirpath, filename)
                if dataset_path not in seen:
                    seen.add(dataset_path)
                    yield dataset_path
    except Exception:
        return


def _extract_utility_network_summary_rows(utility_network_path, describe):
    return [
        {"Property": "Utility Network Path", "Value": utility_network_path},
        {"Property": "Data Type", "Value": getattr(describe, "dataType", "")},
        {"Property": "Creation Time", "Value": _stringify_value(getattr(describe, "creationTime", None))},
        {"Property": "Pro Version", "Value": _stringify_value(getattr(describe, "proVersion", None))},
        {"Property": "Global ID", "Value": _stringify_value(_safe_getattr(describe, "globalID", "globalId"))},
        {"Property": "Schema Generation", "Value": _stringify_value(getattr(describe, "schemaGeneration", None))},
        {"Property": "Minimal Dirty Area Size", "Value": _stringify_value(getattr(describe, "minimalDirtyAreaSize", None))},
        {
            "Property": "Create Dirty Area For Any Attribute Update",
            "Value": _stringify_value(getattr(describe, "createDirtyAreaForAnyAttributeUpdate", None)),
        },
        {
            "Property": "Service Territory Feature Class Name",
            "Value": _stringify_value(getattr(describe, "serviceTerritoryFeatureClassName", None)),
        },
        {"Property": "Association Source Name", "Value": _stringify_value(_safe_getattr(getattr(describe, "associationSource", None), "name"))},
        {"Property": "Association Source ID", "Value": _stringify_value(_safe_getattr(getattr(describe, "associationSource", None), "sourceID", "sourceId"))},
        {"Property": "Association Source Type", "Value": _stringify_value(_safe_getattr(getattr(describe, "associationSource", None), "sourceType"))},
        {"Property": "System Junction Source Name", "Value": _stringify_value(_safe_getattr(getattr(describe, "systemJunctionSource", None), "name"))},
        {"Property": "System Junction Source ID", "Value": _stringify_value(_safe_getattr(getattr(describe, "systemJunctionSource", None), "sourceID", "sourceId"))},
        {"Property": "System Junction Source Type", "Value": _stringify_value(_safe_getattr(getattr(describe, "systemJunctionSource", None), "sourceType"))},
        {"Property": "System Junction Object Source Name", "Value": _stringify_value(_safe_getattr(getattr(describe, "systemJunctionObjectSource", None), "name"))},
        {"Property": "System Junction Object Source ID", "Value": _stringify_value(_safe_getattr(getattr(describe, "systemJunctionObjectSource", None), "sourceID", "sourceId"))},
        {"Property": "System Junction Object Source Type", "Value": _stringify_value(_safe_getattr(getattr(describe, "systemJunctionObjectSource", None), "sourceType"))},
    ]


def _extract_association_and_system_source_rows(utility_network_path, describe):
    rows = []
    source_specs = [
        ("Association Source", getattr(describe, "associationSource", None)),
        ("System Junction Source", getattr(describe, "systemJunctionSource", None)),
        ("System Junction Object Source", getattr(describe, "systemJunctionObjectSource", None)),
    ]
    for source_kind, source in source_specs:
        if source is None:
            continue
        rows.append(
            {
                "Utility Network Path": utility_network_path,
                "Source Kind": source_kind,
                "Name": _stringify_value(_safe_getattr(source, "name", "sourceName")),
                "Source ID": _stringify_value(_safe_getattr(source, "sourceID", "sourceId")),
                "Source Type": _stringify_value(_safe_getattr(source, "sourceType")),
            }
        )
    return rows


def _extract_domain_network_rows(utility_network_path, describe):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        tier_groups = [_stringify_value(_safe_getattr(item, "name")) for item in (_safe_getattr(domain_network, "tierGroups", default=[]) or [])]
        tiers = [_stringify_value(_safe_getattr(item, "name")) for item in (_safe_getattr(domain_network, "tiers", default=[]) or [])]
        edge_sources = [_stringify_value(_safe_getattr(item, "name")) for item in (_safe_getattr(domain_network, "edgeSources", default=[]) or [])]
        junction_sources = [_stringify_value(_safe_getattr(item, "name")) for item in (_safe_getattr(domain_network, "junctionSources", default=[]) or [])]
        rows.append(
            {
                "Utility Network Path": utility_network_path,
                "Domain Network ID": _stringify_value(_safe_getattr(domain_network, "domainNetworkID", "domainNetworkId")),
                "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                "Domain Network Alias": _stringify_value(_safe_getattr(domain_network, "domainNetworkAliasName")),
                "Creation Time": _stringify_value(_safe_getattr(domain_network, "creationTime")),
                "Release Number": _stringify_value(_safe_getattr(domain_network, "releaseNumber")),
                "Is Structure Network": _stringify_value(_safe_getattr(domain_network, "isStructureNetwork")),
                "Subnetwork Table Name": _stringify_value(_safe_getattr(domain_network, "subnetworkTableName")),
                "Subnetwork Label Field Name": _stringify_value(_safe_getattr(domain_network, "subnetworkLabelFieldName")),
                "Tier Definition": _stringify_value(_safe_getattr(domain_network, "tierDefinition")),
                "Subnetwork Controller Type": _stringify_value(_safe_getattr(domain_network, "subnetworkControllerType")),
                "Tier Groups": "; ".join(tier_groups),
                "Tiers": "; ".join(tiers),
                "Edge Sources": "; ".join(edge_sources),
                "Junction Sources": "; ".join(junction_sources),
            }
        )
    return rows


def _extract_tier_group_rows(utility_network_path, describe):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for tier_group in _safe_getattr(domain_network, "tierGroups", default=[]) or []:
            rows.append(
                {
                    "Utility Network Path": utility_network_path,
                    "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                    "Tier Group Name": _stringify_value(_safe_getattr(tier_group, "name")),
                    "Creation Time": _stringify_value(_safe_getattr(tier_group, "creationTime")),
                }
            )
    return rows


def _extract_tier_rows(utility_network_path, describe):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for tier in _safe_getattr(domain_network, "tiers", default=[]) or []:
            manage_subnetwork = _safe_getattr(tier, "manageSubnetwork")
            aggregated_lines = _safe_getattr(tier, "aggregatedLinesForSubnetLine")
            rows.append(
                {
                    "Utility Network Path": utility_network_path,
                    "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                    "Tier Name": _stringify_value(_safe_getattr(tier, "name")),
                    "Tier Group Name": _stringify_value(_safe_getattr(tier, "tierGroupName")),
                    "Rank": _stringify_value(_safe_getattr(tier, "rank")),
                    "Creation Time": _stringify_value(_safe_getattr(tier, "creationTime")),
                    "Subnetwork Field Name": _stringify_value(_safe_getattr(tier, "subnetworkFieldName")),
                    "Diagram Templates": _stringify_value(_safe_getattr(tier, "diagramTemplates")),
                    "Update Subnetwork Policy": _stringify_value(_safe_getattr(manage_subnetwork, "updateSubnetworkPolicy")),
                    "Subnetwork Name Suffix": _stringify_value(_safe_getattr(manage_subnetwork, "subnetworkNameSuffix")),
                    "Include Structure": _stringify_value(_safe_getattr(aggregated_lines, "includeStructure")),
                    "Use Digitized Direction": _stringify_value(_safe_getattr(aggregated_lines, "useDigitizedDirection")),
                }
            )
    return rows


def _extract_source_rows(utility_network_path, describe, source_attr_name, source_kind):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for source in _safe_getattr(domain_network, source_attr_name, default=[]) or []:
            asset_groups = []
            for asset_group in _safe_getattr(source, "assetGroups", default=[]) or []:
                asset_groups.append(
                    _stringify_value(
                        _safe_getattr(asset_group, "assetGroupName", "name")
                    )
                )
            rows.append(
                {
                    "Utility Network Path": utility_network_path,
                    "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                    "Source Kind": source_kind,
                    "Source Name": _stringify_value(_safe_getattr(source, "sourceName", "name")),
                    "Source ID": _stringify_value(_safe_getattr(source, "sourceID", "sourceId")),
                    "Object Class ID": _stringify_value(_safe_getattr(source, "objectClassID", "objectClassId")),
                    "Shape Type": _stringify_value(_safe_getattr(source, "shapeType")),
                    "Asset Type Field Name": _stringify_value(_safe_getattr(source, "assetTypeFieldName")),
                    "Uses Geometry": _stringify_value(_safe_getattr(source, "usesGeometry")),
                    "Usage Type": _stringify_value(_safe_getattr(source, "utilityNetworkFeatureClassUsageType")),
                    "Supported Properties": _stringify_value(_safe_getattr(source, "supportedProperties")),
                    "Asset Groups": "; ".join(asset_groups),
                }
            )
    return rows


def _extract_edge_source_rows(utility_network_path, describe):
    return _extract_source_rows(utility_network_path, describe, "edgeSources", "Edge Source")


def _extract_junction_source_rows(utility_network_path, describe):
    return _extract_source_rows(utility_network_path, describe, "junctionSources", "Junction Source")


def _extract_domain_rows(utility_network_path, workspace_path):
    rows = []
    for domain in arcpy.da.ListDomains(workspace_path):
        domain_header = {
            "Utility Network Path": utility_network_path,
            "Workspace Path": workspace_path,
            "Domain Name": _stringify_value(getattr(domain, "name", None)),
            "Domain Type": _stringify_value(getattr(domain, "domainType", None)),
            "Field Type": _stringify_value(getattr(domain, "type", None)),
            "Description": _stringify_value(getattr(domain, "description", None)),
            "Split Policy": _stringify_value(getattr(domain, "splitPolicy", None)),
            "Merge Policy": _stringify_value(getattr(domain, "mergePolicy", None)),
            "Range Min": "",
            "Range Max": "",
            "Code": "",
            "Value": "",
        }

        if getattr(domain, "domainType", None) == "Range":
            domain_range = getattr(domain, "range", None) or ["", ""]
            row = dict(domain_header)
            row["Range Min"] = _stringify_value(domain_range[0] if len(domain_range) > 0 else "")
            row["Range Max"] = _stringify_value(domain_range[1] if len(domain_range) > 1 else "")
            rows.append(row)
            continue

        coded_values = getattr(domain, "codedValues", None) or {}
        if coded_values:
            for code, value in coded_values.items():
                row = dict(domain_header)
                row["Code"] = _stringify_value(code)
                row["Value"] = _stringify_value(value)
                rows.append(row)
        else:
            rows.append(domain_header)
    return rows


def _extract_domain_assignment_rows(utility_network_path, workspace_path):
    rows = []
    for dataset_path in _iter_workspace_datasets(workspace_path):
        try:
            desc = arcpy.Describe(dataset_path)
            dataset_type = getattr(desc, "dataType", "")
            fields = {field.name: field for field in arcpy.ListFields(dataset_path)}
            subtypes = arcpy.da.ListSubtypes(dataset_path)
        except Exception:
            continue

        for subtype_code, subtype_info in subtypes.items():
            subtype_field = subtype_info.get("SubtypeField", "") or ""
            subtype_name = subtype_info.get("Name", "") or ""
            field_values = subtype_info.get("FieldValues", {}) or {}
            assignment_level = "Subtype" if subtype_field else "Class"

            for field_name, field in fields.items():
                default_value = None
                domain = None

                if field_name in field_values:
                    default_value, domain = field_values[field_name]

                domain_name = _stringify_value(getattr(domain, "name", None) or getattr(field, "domain", None))
                if not domain_name:
                    continue

                rows.append(
                    {
                        "Utility Network Path": utility_network_path,
                        "Workspace Path": workspace_path,
                        "Dataset Path": dataset_path,
                        "Dataset Type": dataset_type,
                        "Class Name": getattr(desc, "name", os.path.basename(dataset_path)),
                        "Subtype Field": subtype_field,
                        "Subtype Code": "" if not subtype_field else _stringify_value(subtype_code),
                        "Subtype Name": "" if not subtype_field else subtype_name,
                        "Assignment Level": assignment_level,
                        "Field Name": field_name,
                        "Field Type": _stringify_value(getattr(field, "type", None)),
                        "Domain Name": domain_name,
                        "Default Code": _stringify_value(default_value),
                        "Default Value": _lookup_domain_description(domain, default_value),
                    }
                )
    return rows


def _extract_asset_group_rows(utility_network_path, describe):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for source_attr_name, source_kind in [("edgeSources", "Edge Source"), ("junctionSources", "Junction Source")]:
            for source in _safe_getattr(domain_network, source_attr_name, default=[]) or []:
                for asset_group in _safe_getattr(source, "assetGroups", default=[]) or []:
                    rows.append(
                        {
                            "Utility Network Path": utility_network_path,
                            "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                            "Source Kind": source_kind,
                            "Source Name": _stringify_value(_safe_getattr(source, "sourceName", "name")),
                            "Source ID": _stringify_value(_safe_getattr(source, "sourceID", "sourceId")),
                            "Asset Group Code": _stringify_value(_safe_getattr(asset_group, "assetGroupCode")),
                            "Asset Group Name": _stringify_value(_safe_getattr(asset_group, "assetGroupName")),
                            "Creation Time": _stringify_value(_safe_getattr(asset_group, "creationTime")),
                        }
                    )
    return rows


def _extract_asset_type_rows(utility_network_path, describe):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for source_attr_name, source_kind in [("edgeSources", "Edge Source"), ("junctionSources", "Junction Source")]:
            for source in _safe_getattr(domain_network, source_attr_name, default=[]) or []:
                for asset_group in _safe_getattr(source, "assetGroups", default=[]) or []:
                    for asset_type in _safe_getattr(asset_group, "assetTypes", default=[]) or []:
                        categories = "; ".join(_stringify_value(item) for item in _normalize_to_list(_safe_getattr(asset_type, "categories")))
                        rows.append(
                            {
                                "Utility Network Path": utility_network_path,
                                "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                                "Source Kind": source_kind,
                                "Source Name": _stringify_value(_safe_getattr(source, "sourceName", "name")),
                                "Asset Group Code": _stringify_value(_safe_getattr(asset_group, "assetGroupCode")),
                                "Asset Group Name": _stringify_value(_safe_getattr(asset_group, "assetGroupName")),
                                "Asset Type Code": _stringify_value(_safe_getattr(asset_type, "assetTypeCode")),
                                "Asset Type Name": _stringify_value(_safe_getattr(asset_type, "assetTypeName")),
                                "Creation Time": _stringify_value(_safe_getattr(asset_type, "creationTime")),
                                "Association Role Type": _stringify_value(_safe_getattr(asset_type, "associationRoleType")),
                                "Association Delete Type": _stringify_value(_safe_getattr(asset_type, "associationDeleteType")),
                                "Containment View Scale": _stringify_value(_safe_getattr(asset_type, "containmentViewScale")),
                                "Connectivity Policy": _stringify_value(_safe_getattr(asset_type, "connectivityPolicy")),
                                "Split Content": _stringify_value(_safe_getattr(asset_type, "splitContent")),
                                "Is Terminal Configuration Supported": _stringify_value(_safe_getattr(asset_type, "isTerminalConfigurationSupported")),
                                "Terminal Configuration ID": _stringify_value(_safe_getattr(asset_type, "terminalConfigurationId")),
                                "Categories": categories,
                            }
                        )
    return rows


def _extract_network_category_assignment_rows(utility_network_path, describe):
    rows = []
    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for source_attr_name, source_kind in [("edgeSources", "Edge Source"), ("junctionSources", "Junction Source")]:
            for source in _safe_getattr(domain_network, source_attr_name, default=[]) or []:
                for asset_group in _safe_getattr(source, "assetGroups", default=[]) or []:
                    for asset_type in _safe_getattr(asset_group, "assetTypes", default=[]) or []:
                        for category in _normalize_to_list(_safe_getattr(asset_type, "categories")):
                            rows.append(
                                {
                                    "Utility Network Path": utility_network_path,
                                    "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                                    "Source Kind": source_kind,
                                    "Source Name": _stringify_value(_safe_getattr(source, "sourceName", "name")),
                                    "Asset Group Code": _stringify_value(_safe_getattr(asset_group, "assetGroupCode")),
                                    "Asset Group Name": _stringify_value(_safe_getattr(asset_group, "assetGroupName")),
                                    "Asset Type Code": _stringify_value(_safe_getattr(asset_type, "assetTypeCode")),
                                    "Asset Type Name": _stringify_value(_safe_getattr(asset_type, "assetTypeName")),
                                    "Network Category": _stringify_value(category),
                                }
                            )
    return rows


def _extract_network_attribute_rows(utility_network_path, describe):
    rows = []
    for attribute in getattr(describe, "networkAttributes", []) or []:
        assignments = []
        for assignment in _safe_getattr(attribute, "assignments", default=[]) or []:
            evaluator = _safe_getattr(assignment, "evaluator")
            assignments.append(
                "{0}|{1}|{2}|{3}".format(
                    _stringify_value(_safe_getattr(assignment, "networkSourceName")),
                    _stringify_value(_safe_getattr(assignment, "networkAttributeId")),
                    _stringify_value(_safe_getattr(evaluator, "evaluatorType")),
                    _stringify_value(_safe_getattr(evaluator, "fieldName")),
                )
            )

        rows.append(
            {
                "Utility Network Path": utility_network_path,
                "ID": _stringify_value(_safe_getattr(attribute, "Id", "id")),
                "Name": _stringify_value(_safe_getattr(attribute, "name")),
                "Data Type": _stringify_value(_safe_getattr(attribute, "dataType")),
                "Usage Type": _stringify_value(_safe_getattr(attribute, "usageType")),
                "Is Embedded": _stringify_value(_safe_getattr(attribute, "isEmbedded")),
                "Is Apportionable": _stringify_value(_safe_getattr(attribute, "isApportionable")),
                "Is Overridable": _stringify_value(_safe_getattr(attribute, "isOverridable")),
                "Domain Name": _stringify_value(_safe_getattr(attribute, "domainName")),
                "Bit Position": _stringify_value(_safe_getattr(attribute, "bitPosition")),
                "Bit Size": _stringify_value(_safe_getattr(attribute, "bitSize")),
                "Junction Weight ID": _stringify_value(_safe_getattr(attribute, "junctionWeightId")),
                "Edge Weight ID": _stringify_value(_safe_getattr(attribute, "edgeWeightId")),
                "Assignments": "; ".join(assignments),
            }
        )
    return rows


def _extract_network_attribute_assignment_rows(utility_network_path, describe):
    rows = []
    for attribute in getattr(describe, "networkAttributes", []) or []:
        assignments = _safe_getattr(attribute, "assignments", default=[]) or []
        if not assignments:
            rows.append(
                {
                    "Utility Network Path": utility_network_path,
                    "Network Attribute Name": _stringify_value(_safe_getattr(attribute, "name")),
                    "Network Attribute ID": _stringify_value(_safe_getattr(attribute, "Id", "id")),
                    "Network Source Name": "",
                    "Assignment Attribute ID": "",
                    "Evaluator Type": "",
                    "Evaluator Field Name": "",
                }
            )
            continue

        for assignment in assignments:
            evaluator = _safe_getattr(assignment, "evaluator")
            rows.append(
                {
                    "Utility Network Path": utility_network_path,
                    "Network Attribute Name": _stringify_value(_safe_getattr(attribute, "name")),
                    "Network Attribute ID": _stringify_value(_safe_getattr(attribute, "Id", "id")),
                    "Network Source Name": _stringify_value(_safe_getattr(assignment, "networkSourceName")),
                    "Assignment Attribute ID": _stringify_value(_safe_getattr(assignment, "networkAttributeId")),
                    "Evaluator Type": _stringify_value(_safe_getattr(evaluator, "evaluatorType")),
                    "Evaluator Field Name": _stringify_value(_safe_getattr(evaluator, "fieldName")),
                }
            )
    return rows


def _extract_category_rows(utility_network_path, describe):
    rows = []
    for category in getattr(describe, "categories", []) or []:
        rows.append(
            {
                "Utility Network Path": utility_network_path,
                "Category Name": _stringify_value(_safe_getattr(category, "name")),
                "Creation Time": _stringify_value(_safe_getattr(category, "creationTime")),
            }
        )
    return rows


def _extract_terminal_configuration_rows(utility_network_path, describe):
    rows = []
    for config in getattr(describe, "terminalConfigurations", []) or []:
        terminals = []
        for terminal in _safe_getattr(config, "terminals", default=[]) or []:
            terminals.append(
                "{0}|{1}|{2}".format(
                    _stringify_value(_safe_getattr(terminal, "terminalId")),
                    _stringify_value(_safe_getattr(terminal, "terminalName")),
                    _stringify_value(_safe_getattr(terminal, "isUpstreamTerminal")),
                )
            )

        valid_configurations = []
        valid_paths = _safe_getattr(config, "validConfigurations", "validConfigurationPaths", default=[]) or []
        for valid_config in valid_paths:
            terminal_paths = []
            for terminal_path in _safe_getattr(valid_config, "terminalPaths", default=[]) or []:
                terminal_paths.append(
                    "{0}->{1}".format(
                        _stringify_value(_safe_getattr(terminal_path, "fromTerminalId")),
                        _stringify_value(_safe_getattr(terminal_path, "toTerminalId")),
                    )
                )
            valid_configurations.append(
                "{0}|{1}|{2}|{3}".format(
                    _stringify_value(_safe_getattr(valid_config, "id")),
                    _stringify_value(_safe_getattr(valid_config, "name")),
                    _stringify_value(_safe_getattr(valid_config, "description")),
                    ", ".join(terminal_paths),
                )
            )

        rows.append(
            {
                "Utility Network Path": utility_network_path,
                "Terminal Configuration ID": _stringify_value(_safe_getattr(config, "terminalConfigurationID", "terminalConfigurationId")),
                "Terminal Configuration Name": _stringify_value(_safe_getattr(config, "terminalConfigurationName")),
                "Creation Time": _stringify_value(_safe_getattr(config, "creationTime")),
                "Traversability Model": _stringify_value(_safe_getattr(config, "traversabilityModel")),
                "Default Configuration": _stringify_value(_safe_getattr(config, "defaultConfiguration")),
                "Terminals": "; ".join(terminals),
                "Valid Configurations": "; ".join(valid_configurations),
            }
        )
    return rows


def _extract_terminal_rows(utility_network_path, describe):
    rows = []
    for config in getattr(describe, "terminalConfigurations", []) or []:
        for terminal in _safe_getattr(config, "terminals", default=[]) or []:
            rows.append(
                {
                    "Utility Network Path": utility_network_path,
                    "Terminal Configuration Name": _stringify_value(_safe_getattr(config, "terminalConfigurationName")),
                    "Terminal Configuration ID": _stringify_value(_safe_getattr(config, "terminalConfigurationID", "terminalConfigurationId")),
                    "Terminal ID": _stringify_value(_safe_getattr(terminal, "terminalId", "terminalID")),
                    "Terminal Name": _stringify_value(_safe_getattr(terminal, "terminalName")),
                    "Is Upstream Terminal": _stringify_value(_safe_getattr(terminal, "isUpstreamTerminal")),
                }
            )
    return rows


def _extract_terminal_assignment_rows(utility_network_path, describe):
    rows = []
    configuration_names = {}
    for config in getattr(describe, "terminalConfigurations", []) or []:
        config_id = _stringify_value(_safe_getattr(config, "terminalConfigurationID", "terminalConfigurationId"))
        configuration_names[config_id] = _stringify_value(_safe_getattr(config, "terminalConfigurationName"))

    for domain_network in getattr(describe, "domainNetworks", []) or []:
        for source_attr_name, source_kind in [("edgeSources", "Edge Source"), ("junctionSources", "Junction Source")]:
            for source in _safe_getattr(domain_network, source_attr_name, default=[]) or []:
                for asset_group in _safe_getattr(source, "assetGroups", default=[]) or []:
                    for asset_type in _safe_getattr(asset_group, "assetTypes", default=[]) or []:
                        terminal_configuration_id = _stringify_value(
                            _safe_getattr(asset_type, "terminalConfigurationId", "terminalConfigurationID")
                        )
                        if not terminal_configuration_id and not _safe_getattr(asset_type, "isTerminalConfigurationSupported"):
                            continue

                        rows.append(
                            {
                                "Utility Network Path": utility_network_path,
                                "Domain Network Name": _stringify_value(_safe_getattr(domain_network, "domainNetworkName")),
                                "Source Kind": source_kind,
                                "Source Name": _stringify_value(_safe_getattr(source, "sourceName", "name")),
                                "Asset Group Code": _stringify_value(_safe_getattr(asset_group, "assetGroupCode")),
                                "Asset Group Name": _stringify_value(_safe_getattr(asset_group, "assetGroupName")),
                                "Asset Type Code": _stringify_value(_safe_getattr(asset_type, "assetTypeCode")),
                                "Asset Type Name": _stringify_value(_safe_getattr(asset_type, "assetTypeName")),
                                "Terminal Configuration Supported": _stringify_value(
                                    _safe_getattr(asset_type, "isTerminalConfigurationSupported")
                                ),
                                "Terminal Configuration ID": terminal_configuration_id,
                                "Terminal Configuration Name": configuration_names.get(terminal_configuration_id, ""),
                            }
                        )
    return rows


def _extract_valid_configuration_path_rows(utility_network_path, describe):
    rows = []
    for config in getattr(describe, "terminalConfigurations", []) or []:
        valid_paths = _safe_getattr(config, "validConfigurations", "validConfigurationPaths", default=[]) or []
        for valid_config in valid_paths:
            terminal_paths = _safe_getattr(valid_config, "terminalPaths", default=[]) or []
            if not terminal_paths:
                rows.append(
                    {
                        "Utility Network Path": utility_network_path,
                        "Terminal Configuration Name": _stringify_value(_safe_getattr(config, "terminalConfigurationName")),
                        "Configuration ID": _stringify_value(_safe_getattr(valid_config, "id")),
                        "Configuration Name": _stringify_value(_safe_getattr(valid_config, "name")),
                        "Description": _stringify_value(_safe_getattr(valid_config, "description")),
                        "From Terminal ID": "",
                        "To Terminal ID": "",
                    }
                )
                continue

            for terminal_path in terminal_paths:
                rows.append(
                    {
                        "Utility Network Path": utility_network_path,
                        "Terminal Configuration Name": _stringify_value(_safe_getattr(config, "terminalConfigurationName")),
                        "Configuration ID": _stringify_value(_safe_getattr(valid_config, "id")),
                        "Configuration Name": _stringify_value(_safe_getattr(valid_config, "name")),
                        "Description": _stringify_value(_safe_getattr(valid_config, "description")),
                        "From Terminal ID": _stringify_value(_safe_getattr(terminal_path, "fromTerminalId")),
                        "To Terminal ID": _stringify_value(_safe_getattr(terminal_path, "toTerminalId")),
                    }
                )
    return rows


def _export_utility_network_properties(output_folder, selections, map_name=None, utility_network_path=None):
    utility_network_path = _resolve_utility_network_path(map_name, utility_network_path)
    describe, feature_dataset_path, workspace_path = _resolve_utility_network_context(utility_network_path)

    export_plan = {
        "Utility Network Summary": ("UtilityNetworkSummary", _extract_utility_network_summary_rows(utility_network_path, describe)),
        "Association And System Sources": ("AssociationAndSystemSources", _extract_association_and_system_source_rows(utility_network_path, describe)),
        "Domain Networks": ("DomainNetworks", _extract_domain_network_rows(utility_network_path, describe)),
        "Tier Groups": ("TierGroups", _extract_tier_group_rows(utility_network_path, describe)),
        "Tiers": ("Tiers", _extract_tier_rows(utility_network_path, describe)),
        "Edge Sources": ("EdgeSources", _extract_edge_source_rows(utility_network_path, describe)),
        "Junction Sources": ("JunctionSources", _extract_junction_source_rows(utility_network_path, describe)),
        "Domains": ("Domains", _extract_domain_rows(utility_network_path, workspace_path)),
        "Domain Assignments": ("DomainAssignments", _extract_domain_assignment_rows(utility_network_path, workspace_path)),
        "Asset Groups": ("AssetGroups", _extract_asset_group_rows(utility_network_path, describe)),
        "Asset Types": ("AssetTypes", _extract_asset_type_rows(utility_network_path, describe)),
        "Network Category Assignments": (
            "NetworkCategoryAssignments",
            _extract_network_category_assignment_rows(utility_network_path, describe),
        ),
        "Network Attributes": ("NetworkAttributes", _extract_network_attribute_rows(utility_network_path, describe)),
        "Network Attribute Assignments": (
            "NetworkAttributeAssignments",
            _extract_network_attribute_assignment_rows(utility_network_path, describe),
        ),
        "Categories": ("Categories", _extract_category_rows(utility_network_path, describe)),
        "Terminal Configurations": ("TerminalConfigurations", _extract_terminal_configuration_rows(utility_network_path, describe)),
        "Terminal Assignments": ("TerminalAssignments", _extract_terminal_assignment_rows(utility_network_path, describe)),
        "Terminals": ("Terminals", _extract_terminal_rows(utility_network_path, describe)),
        "Valid Configuration Paths": (
            "ValidConfigurationPaths",
            _extract_valid_configuration_path_rows(utility_network_path, describe),
        ),
    }

    selected_exports = selections or list(export_plan.keys())
    for selection in selected_exports:
        if selection not in export_plan:
            arcpy.AddWarning("Skipping unsupported export selection: {0}".format(selection))
            continue
        suffix, rows = export_plan[selection]
        output_file = _utility_network_output_file(output_folder, utility_network_path, suffix)
        _write_rows_to_csv(output_file, rows or [{"Message": "No records found."}])
        arcpy.AddMessage("Wrote {0}".format(output_file))


def _apply_display_expression(expression, title, apply_to_popups, map_name=None, target_member_names=None):
    members = _get_all_supported_members(map_name)
    selected_names = {name.lower() for name in (target_member_names or [])}
    if selected_names:
        members = [record for record in members if record.long_name.lower() in selected_names]
    updated_count = 0

    for record in members:
        expression_info = arcpy.cim.CreateCIMObjectFromClassName("CIMExpressionInfo", TARGET_CIM_VERSION)
        expression_info.title = title
        expression_info.expression = expression
        _set_display_expression_info(record, expression_info)

        if apply_to_popups:
            popup_info = _get_popup_info(record)
            if popup_info is None:
                popup_info = arcpy.cim.CreateCIMObjectFromClassName("CIMPopupInfo", TARGET_CIM_VERSION)

            expression_infos = list(getattr(popup_info, "expressionInfos", []) or [])
            existing = None
            for candidate in expression_infos:
                if getattr(candidate, "name", "") == DISPLAY_EXPRESSION_POPUP_NAME:
                    existing = candidate
                    break

            if existing is None:
                existing = arcpy.cim.CreateCIMObjectFromClassName("CIMExpressionInfo", TARGET_CIM_VERSION)
                existing.name = DISPLAY_EXPRESSION_POPUP_NAME
                expression_infos.append(existing)

            existing.name = DISPLAY_EXPRESSION_POPUP_NAME
            existing.title = title
            existing.expression = expression

            popup_info.expressionInfos = expression_infos
            popup_info.title = "{expression/%s}" % DISPLAY_EXPRESSION_POPUP_NAME
            _set_popup_info(record, popup_info)
            record.definition.showPopups = True

        _save_record_definition(record)
        updated_count += 1

    arcpy.AddMessage("Applied the display expression to {0} map members.".format(updated_count))


def _create_map_parameter():
    parameter = arcpy.Parameter(
        displayName="Map",
        name="map_name",
        datatype="GPString",
        parameterType="Optional",
        direction="Input",
    )
    parameter.filter.type = "ValueList"
    return parameter


def _create_multivalue_string_parameter(display_name, name):
    parameter = arcpy.Parameter(
        displayName=display_name,
        name=name,
        datatype="GPString",
        parameterType="Optional",
        direction="Input",
        multiValue=True,
    )
    parameter.filter.type = "ValueList"
    parameter.controlCLSID = MULTIVALUE_CHECKBOX_SELECT_ALL_CONTROL
    return parameter


def _refresh_map_parameter(parameter):
    try:
        map_names = _get_map_names()
    except ToolboxError:
        map_names = []

    parameter.filter.list = map_names
    if not parameter.altered and map_names:
        try:
            _, active_map = _get_active_map()
            parameter.value = active_map.name
        except ToolboxError:
            parameter.value = map_names[0]


def _refresh_member_parameter(parameter, map_name=None):
    try:
        member_names = _get_selectable_member_names(map_name)
    except ToolboxError:
        member_names = []
    parameter.filter.list = member_names


def _refresh_utility_network_parameter(parameter, map_name=None):
    return


def _execute_with_error_boundary(callback):
    try:
        callback()
    except ToolboxError:
        raise
    except Exception as exc:
        raise ToolboxError("{0}\n\n{1}".format(exc, traceback.format_exc())) from exc


class Toolbox(object):
    def __init__(self):
        self.label = "Utility Network Map Tools"
        self.alias = "utilitynetworkmaptools"
        self.description = (
            "ArcGIS Pro {0} Python toolbox for exporting and applying map field "
            "settings and display expressions."
        ).format(TARGET_ARCGIS_PRO)
        self.tools = [
            ExportFieldSettingsTool,
            ImportFieldSettingsTool,
            ExportLayerScalesTool,
            ImportLayerScalesTool,
            ApplyDisplayExpressionTool,
            ExportUtilityNetworkPropertiesTool,
        ]


class _BaseTool(object):
    canRunInBackground = False

    def isLicensed(self):
        return True

    def updateMessages(self, parameters):
        return


class ExportFieldSettingsTool(_BaseTool):
    def __init__(self):
        self.label = "Export Current Map Field Settings"
        self.description = (
            "Exports the active map's feature-layer and standalone-table field settings, "
            "including field order, alias, visibility, read-only status, and highlight status."
        )

    def getParameterInfo(self):
        map_name = _create_map_parameter()
        output_file = arcpy.Parameter(
            displayName="Output CSV File",
            name="output_csv_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Output",
        )
        output_file.filter.list = ["csv"]
        return [map_name, output_file]

    def updateParameters(self, parameters):
        _refresh_map_parameter(parameters[0])
        if not parameters[1].altered:
            try:
                parameters[1].value = _get_default_output_file("FieldSettings", "csv", parameters[0].valueAsText)
            except ToolboxError:
                pass

    def execute(self, parameters, messages):
        map_name = parameters[0].valueAsText
        output_file = parameters[1].valueAsText
        _execute_with_error_boundary(lambda: _write_field_settings_csv(output_file, map_name))


class ImportFieldSettingsTool(_BaseTool):
    def __init__(self):
        self.label = "Apply Field Settings From CSV"
        self.description = (
            "Reads a field-settings CSV and applies the edited field order, alias, visibility, "
            "read-only, and highlight values back to the active map."
        )

    def getParameterInfo(self):
        map_name = _create_map_parameter()
        input_file = arcpy.Parameter(
            displayName="Input CSV File",
            name="input_csv_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
        )
        input_file.filter.list = ["csv"]
        return [map_name, input_file]

    def updateParameters(self, parameters):
        _refresh_map_parameter(parameters[0])

    def execute(self, parameters, messages):
        map_name = parameters[0].valueAsText
        input_file = parameters[1].valueAsText
        _execute_with_error_boundary(lambda: _apply_field_settings_csv(input_file, map_name))


class ExportLayerScalesTool(_BaseTool):
    def __init__(self):
        self.label = "Export Layer Scales"
        self.description = (
            "Exports the active map's layer visibility scales and a summarized label scale range to CSV."
        )

    def getParameterInfo(self):
        map_name = _create_map_parameter()
        output_file = arcpy.Parameter(
            displayName="Output CSV File",
            name="output_csv_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Output",
        )
        output_file.filter.list = ["csv"]
        return [map_name, output_file]

    def updateParameters(self, parameters):
        _refresh_map_parameter(parameters[0])
        if not parameters[1].altered:
            try:
                parameters[1].value = _get_default_output_file("LayerScales", "csv", parameters[0].valueAsText)
            except ToolboxError:
                pass

    def execute(self, parameters, messages):
        map_name = parameters[0].valueAsText
        output_file = parameters[1].valueAsText
        _execute_with_error_boundary(lambda: _write_layer_scales_csv(output_file, map_name))


class ImportLayerScalesTool(_BaseTool):
    def __init__(self):
        self.label = "Apply Layer Scales From CSV"
        self.description = (
            "Reads a layer-scale CSV and applies layer min and max scales back to the selected map. "
            "If label scale columns are edited, the same range is applied to all label classes on the layer."
        )

    def getParameterInfo(self):
        map_name = _create_map_parameter()
        input_file = arcpy.Parameter(
            displayName="Input CSV File",
            name="input_csv_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
        )
        input_file.filter.list = ["csv"]
        return [map_name, input_file]

    def updateParameters(self, parameters):
        _refresh_map_parameter(parameters[0])

    def execute(self, parameters, messages):
        map_name = parameters[0].valueAsText
        input_file = parameters[1].valueAsText
        _execute_with_error_boundary(lambda: _apply_layer_scales_csv(input_file, map_name))


class ExportUtilityNetworkPropertiesTool(_BaseTool):
    def __init__(self):
        self.label = "Export Utility Network Properties"
        self.description = (
            "Exports selected utility network property groups to CSV files in a chosen output folder."
        )

    def getParameterInfo(self):
        map_name = _create_map_parameter()
        utility_network = arcpy.Parameter(
            displayName="Utility Network",
            name="utility_network",
            datatype=list(UTILITY_NETWORK_INPUT_DATATYPES),
            parameterType="Optional",
            direction="Input",
        )
        export_options = _create_multivalue_string_parameter("Property Groups", "property_groups")
        output_folder = arcpy.Parameter(
            displayName="Output Folder",
            name="output_folder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input",
        )
        return [map_name, utility_network, export_options, output_folder]

    def updateParameters(self, parameters):
        _refresh_map_parameter(parameters[0])
        _refresh_utility_network_parameter(parameters[1], parameters[0].valueAsText)
        parameters[2].filter.list = list(UN_EXPORT_OPTIONS)
        if not parameters[2].altered:
            parameters[2].value = ";".join(UN_EXPORT_OPTIONS)
        if not parameters[3].altered:
            try:
                aprx = _get_current_project()
                parameters[3].value = aprx.homeFolder or os.getcwd()
            except ToolboxError:
                pass

    def execute(self, parameters, messages):
        map_name = parameters[0].valueAsText
        utility_network_path = parameters[1].valueAsText
        selections = _parse_multivalue_text(parameters[2].valueAsText)
        output_folder = parameters[3].valueAsText
        _execute_with_error_boundary(
            lambda: _export_utility_network_properties(output_folder, selections, map_name, utility_network_path)
        )


class ApplyDisplayExpressionTool(_BaseTool):
    def __init__(self):
        self.label = "Apply Arcade Display Expression"
        self.description = (
            "Applies an Arcade display expression to every feature layer and standalone table "
            "in the active map, with an option to also wire the same expression into popup titles."
        )

    def getParameterInfo(self):
        expression = arcpy.Parameter(
            displayName="Arcade Expression",
            name="arcade_expression",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        expression.controlCLSID = MULTILINE_TEXT_CONTROL
        map_name = _create_map_parameter()
        target_members = _create_multivalue_string_parameter("Target Layers/Tables", "target_members")
        expression_title = arcpy.Parameter(
            displayName="Expression Title",
            name="expression_title",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        apply_to_popups = arcpy.Parameter(
            displayName="Also Apply To Popup Titles",
            name="apply_to_popups",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        apply_to_popups.value = False
        return [expression, map_name, target_members, expression_title, apply_to_popups]

    def updateParameters(self, parameters):
        _refresh_map_parameter(parameters[1])
        _refresh_member_parameter(parameters[2], parameters[1].valueAsText)
        if not parameters[3].altered:
            parameters[3].value = "Display Expression"

    def execute(self, parameters, messages):
        expression = parameters[0].valueAsText
        map_name = parameters[1].valueAsText
        target_members = _parse_multivalue_text(parameters[2].valueAsText)
        title = parameters[3].valueAsText or "Display Expression"
        apply_to_popups = _parse_bool(parameters[4].valueAsText, default=False)
        _execute_with_error_boundary(
            lambda: _apply_display_expression(expression, title, apply_to_popups, map_name, target_members)
        )
