import datetime
from collections import defaultdict
from enum import Enum
from warnings import filterwarnings
from typing import Optional, Dict, Callable, Any, List, Set, Tuple, Type

import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
import openpyxl
from sqlalchemy import Column, Enum as SqlEnum

from .example_models import MetadataModel


filterwarnings(
    action="ignore",
    category=UserWarning,
    message="Data Validation extension is not supported",
    module="openpyxl",
)


class Entry:
    """
    One field in a metadata worksheet. Provides configuration for reading a
    value from a metadata spreadsheet cell into a sqlalchemy model attribute
    (or set of related attributes).

    Args:
      column: column attribute on a SQLAlchemy model class.
      name: optional human-readable label for this field, if not inferrable from `column`.
      process_as: optional dictionary mapping column attributes to data processing function.
      encrypt: whether the spreadsheet value should be encrypted before storage.
    """

    def __init__(
        self,
        column: Column,
        name: Optional[str] = None,
        gcs_uri_format: Optional[str] = None,
        process_as: Dict[Column, Callable[[str], Any]] = {},
    ):
        self.column = column
        self.name = name if name else column.name.replace("_", " ")
        self.gcs_uri_format = gcs_uri_format
        self.process_as = process_as

        self.doc = column.doc
        self.sqltype = column.type
        self.enums = self.sqltype.enums if isinstance(self.sqltype, SqlEnum) else None
        self.pytype = self.sqltype.python_type

    def get_column_mapping(self, value) -> Dict[Column, Any]:
        if value is None:
            if self.nullable:
                return {self.column: None}
            else:
                raise Exception(f"Missing required value {self.name}")

        try:
            # Handle date/time parsing funkiness
            if self.pytype == datetime.time:
                processed_value = value.time()
            elif self.pytype == datetime.date:
                processed_value = value.date()
            else:
                processed_value = self.pytype(value)

            column_mapping = {self.column: processed_value}

            for column, process in self.process_as.items():
                column_mapping[column] = process(value)
        except Exception as e:
            raise Exception(
                f"Error processing {self.name}={value}({type(value)}) as {self.pytype}: {e}"
            )
        return column_mapping


class WorksheetConfig:
    """
    A worksheet within a metadata spreadsheet. A worksheet has a name, a list
    of `preamble_rows` and a dictionary mapping data column group headers to lists
    of `data_sections`.
    etc. etc...
    """

    name: str
    preamble: List[Entry]
    data_sections: Dict[str, List[Entry]]

    def __init__(
        self, name: str, preamble: List[Entry], data_sections: Dict[str, List[Entry]]
    ):
        self.name = name
        self.preamble = preamble
        self.data_sections = data_sections

        self.distinct_models = self._get_distinct_models()

    def _get_distinct_models(self) -> List[MetadataModel]:
        entries = self.preamble + [
            entry for entries in self.data_sections.values() for entry in entries
        ]

        return list(set([entry.column.class_ for entry in entries]))


class RowType(Enum):
    """Annotations denoting what type of data a template row contains."""

    TITLE = "#title"
    SKIP = "#skip"
    HEADER = "#header"
    PREAMBLE = "#preamble"
    DATA = "#data"


def row_type_from_string(maybe_type: str) -> Optional[RowType]:
    try:
        return RowType(maybe_type)
    except ValueError:
        return None


def _format_validation_range(
    validation_rows, validation_column, data_dict_worksheet_name
):
    start = xl_rowcol_to_cell(
        1,  # 1 is to skip first row in DD sheet that is for header
        validation_column,
        row_abs=True,
        col_abs=True,
    )
    stop = xl_rowcol_to_cell(
        validation_rows, validation_column, row_abs=True, col_abs=True
    )

    return f"'{data_dict_worksheet_name}'!{start}:{stop}"


class ExcelStyles:
    COLUMN_WIDTH = 30

    TITLE_STYLE_PROPS = {
        "border": 1,
        "bg_color": "#ffffb3",
        "bold": True,
        "align": "center",
        "text_wrap": True,
        "valign": "vcenter",
        "indent": 1,
    }
    PREAMBLE_STYLE_PROPS = {
        "border": 0,
        "top": 2,
        "top_color": "white",
        "bottom": 2,
        "bottom_color": "white",
        "bg_color": "#b2d2f6",
        "bold": True,
        "align": "right",
        "text_wrap": True,
        "valign": "vcenter",
        "indent": 1,
    }
    HEADER_STYLE_PROPS = {
        "border": 1,
        "bg_color": "#C6EFCE",
        "bold": True,
        "align": "center",
        "valign": "vcenter",
        "indent": 1,
    }
    DATA_STYLE_PROPS = {
        "border": 1,
        "bg_color": "#5fa3f0",
        "bold": True,
        "align": "center",
        "text_wrap": True,
        "valign": "vcenter",
        "indent": 1,
    }
    DIRECTIVE_STYLE_PROPS = {
        "border": 1,
        "bg_color": "#ffffb3",
        "bold": True,
        "align": "center",
        "text_wrap": True,
        "valign": "vcenter",
        "indent": 1,
    }
    COMMENT_STYLE_PROPS = {
        "color": "white",
        "font_size": 10,
        "x_scale": 2,
        "author": "CIDC",
    }

    def __init__(self, workbook: xlsxwriter.Workbook):
        self.TITLE_STYLE = workbook.add_format(self.TITLE_STYLE_PROPS)
        self.PREAMBLE_STYLE = workbook.add_format(self.PREAMBLE_STYLE_PROPS)
        self.HEADER_STYLE = workbook.add_format(self.HEADER_STYLE_PROPS)
        self.DATA_STYLE = workbook.add_format(self.DATA_STYLE_PROPS)
        self.DIRECTIVE_STYLE = workbook.add_format(self.DIRECTIVE_STYLE_PROPS)


class MetadataTemplate:
    """
    A metadata template. Must have attributes `upload_type` and `worksheets` defined.
    etc. etc...
    """

    upload_type: str
    worksheet_configs: List[WorksheetConfig]

    DATA_ROWS = 2000
    DATA_DICT_SHEETNAME = "Data Dictionary"

    def __init__(self, upload_type: str, worksheet_configs: List[WorksheetConfig]):
        self.upload_type = upload_type
        self.worksheet_configs = worksheet_configs

        self.ordered_models = self._get_model_ordering()

    def _get_model_ordering(self) -> List[Type[MetadataModel]]:
        distinct_models = set(
            model for cfg in self.worksheet_configs for model in cfg.distinct_models
        )
        table_to_model = {m.__tablename__: m for m in distinct_models}

        # Build graph mapping models to their foreign-key dependencies
        fk_graph: Dict[Type[MetadataModel], Set[Type[MetadataModel]]] = defaultdict(set)
        for model in distinct_models:
            for fk in model.__table__.foreign_keys:
                # Models can depend on models not present in the template
                fk_model = table_to_model.get(fk.column.table.name)
                if fk_model:
                    fk_graph[model].add(fk_model)

        print()
        print(fk_graph)

        # Topologically sort the models to get a valid insertion order
        ordered_models = []

        return []

    def read(self, filename: str) -> List[MetadataModel]:
        """
        Extract a list of SQLAlchemy models in insertion order from a populated
        instance of this template.
        """
        workbook = openpyxl.load_workbook(filename)

        model_instances: List[MetadataModel] = []

        # Extract partial model instances from the template
        for config in self.worksheet_configs:
            preamble_rows = []
            data_rows = []
            for row in workbook[config.name].iter_rows():
                row_type = row_type_from_string(row[0].value)
                if row_type == RowType.PREAMBLE:
                    preamble_rows.append(row)
                elif row_type == RowType.DATA:
                    data_rows.append(row)

            if len(preamble_rows) != len(config.preamble):
                raise Exception(
                    f"Expected {len(config.preamble)} preamble rows but saw {len(preamble_rows)}"
                )

            model_dicts: List[Dict[Column, Any]] = []
            preamble_dict = {}
            # {<column instance>: <processed value, ...}
            for i, row in enumerate(preamble_rows):
                cell = row[2].value
                entry = config.preamble[i]
                preamble_dict.update(entry.get_column_mapping(cell))
            model_dicts.append(preamble_dict)

            data_configs = [
                entry for entries in config.data_sections.values() for entry in entries
            ]
            for row in data_rows:
                if all(cell.value is None for cell in row[1:]):
                    continue
                data_dict = {}
                for i, entry in enumerate(data_configs, 1):
                    cell = row[i].value
                    data_dict.update(entry.get_column_mapping(cell))
                model_dicts.append(data_dict)

            for model_dict in model_dicts:
                model_groups = defaultdict(dict)
                for column, value in model_dict.items():
                    model_groups[column.class_][column.name] = value
                for model, kwargs in model_groups.items():
                    model_instances.append(model(**kwargs))

        # Group model instances with matching values in their primary key or
        # unique-constrained columns.
        model_groups: Dict[
            Type[MetadataModel], Dict[Tuple, List[MetadataModel]]
        ] = defaultdict(lambda: defaultdict(list))
        for instance in model_instances:
            unique_values = instance.unique_field_values()
            model_groups[instance.__class__][unique_values].append(instance)

        # Build a dictionary mapping model classes to deduplicated instances
        # of that clsass.
        deduped_instances: Dict[Type[MetadataModel], List[MetadataModel]] = defaultdict(
            list
        )
        for model, groups in model_groups.items():
            # special value None for all(unique_values is None)
            broad_instances = groups.pop(None, [])
            for specific_instances in groups.values():
                instance = model()
                for partial_instance in broad_instances + specific_instances:
                    instance.merge(partial_instance)
                deduped_instances[model].append(instance)

        ordered_instances = []

    def write(self, filename: str):
        """
        Generate an empty excel file for this template type.
        """
        workbook = xlsxwriter.Workbook(filename)
        styles = ExcelStyles(workbook)
        self._write_legend(workbook, styles)
        enum_ranges = self._write_data_dict(workbook, styles)
        self._write_worksheets(workbook, styles, enum_ranges)
        workbook.close()

    def _write_legend(self, workbook: xlsxwriter.Workbook, styles: ExcelStyles):
        ws = workbook.add_worksheet("Legend")
        ws.protect()
        ws.set_column(1, 100, width=styles.COLUMN_WIDTH)

        row = 0
        ws.write(row, 1, f"LEGEND", styles.DATA_STYLE)

        for config in self.worksheet_configs:
            row += 1
            ws.write(row, 1, f"Legend for tab {config.name!r}", styles.TITLE_STYLE)

            for entry in config.preamble:
                row += 1
                self._write_legend_item(ws, row, entry, styles.PREAMBLE_STYLE)

            for section_name, section_entries in config.data_sections.items():
                row += 1
                ws.write(
                    row,
                    1,
                    f"Section {section_name!r} of tab {config.name!r}",
                    styles.DIRECTIVE_STYLE,
                )

                for entry in section_entries:
                    row += 1
                    self._write_legend_item(ws, row, entry, styles.HEADER_STYLE)

    def _write_legend_item(
        self,
        ws: xlsxwriter.workbook.Worksheet,
        row: int,
        entry: Entry,
        style: xlsxwriter.workbook.Format,
    ):
        """ Writes a property with its type, description, and example if any."""
        ws.write(row, 1, entry.name, style)
        ws.write(row, 2, entry.sqltype.__class__.__name__)
        ws.write(row, 3, entry.doc)

    def _write_data_dict(
        self, workbook: xlsxwriter.Workbook, styles: ExcelStyles
    ) -> Dict[str, str]:
        ws = workbook.add_worksheet(self.DATA_DICT_SHEETNAME)
        ws.protect()
        ws.set_column(1, 100, width=styles.COLUMN_WIDTH)

        col = 0

        # a result dictionary that maps field names to data dictionary sheet
        # ranges of enum values to be used for validation
        data_dict_mapping = {}

        for config in self.worksheet_configs:
            for entry in config.preamble:
                rows = self._write_data_dict_item(ws, col, entry, styles.PREAMBLE_STYLE)
                if rows > 0:
                    # saving Data Dict range to use for validation
                    data_dict_mapping[entry.name] = _format_validation_range(
                        rows, col, self.DATA_DICT_SHEETNAME
                    )
                    col += 1

            for section_entries in config.data_sections.values():
                for entry in section_entries:
                    rows = self._write_data_dict_item(
                        ws, col, entry, styles.HEADER_STYLE
                    )
                    if rows > 0:
                        # saving Data Dict range to use for validation
                        data_dict_mapping[entry.name] = _format_validation_range(
                            rows, col, self.DATA_DICT_SHEETNAME
                        )
                        col += 1

        return data_dict_mapping

    def _write_data_dict_item(
        self,
        ws: xlsxwriter.workbook.Worksheet,
        col: int,
        entry: Entry,
        style: xlsxwriter.workbook.Format,
    ):
        if entry.enums is None:
            return 0

        # Write the data dict column header
        ws.write(0, col, entry.name, style)

        # Write the data dict column values
        for i, enum_value in enumerate(entry.enums):
            ws.write(1 + i, col, enum_value)

        return len(entry.enums)

    def _write_worksheets(
        self,
        workbook: xlsxwriter.Workbook,
        styles: ExcelStyles,
        enum_ranges: Dict[str, str],
    ):
        """Write content to the given worksheet"""
        for config in self.worksheet_configs:
            ws = workbook.add_worksheet(config.name)
            ws.set_column(0, 100, width=styles.COLUMN_WIDTH)

            row = 0
            col = 1

            # WORKSHEET TITLE
            self._write_type_annotation(ws, row, RowType.TITLE)
            preamble_range = xl_range(row, 1, row, 2)
            ws.merge_range(preamble_range, config.name, styles.TITLE_STYLE)

            # PREAMBLE ROWS
            row += 1
            for entry in config.preamble:
                # Write row type and entity name
                self._write_type_annotation(ws, row, RowType.PREAMBLE)
                ws.write(row, 1, entry.name, styles.PREAMBLE_STYLE)
                self._write_comment(ws, row, 1, entry, styles)

                # Format value cells next to entity name
                ws.write(row, 2, "", styles.PREAMBLE_STYLE)

                # Add data validation if appropriate
                value_cell = xl_rowcol_to_cell(row, 2)
                self._write_validation(ws, value_cell, entry, enum_ranges)
                row += 1

            # DATA SECTION HEADERS
            row += 1
            self._write_type_annotation(ws, row, RowType.SKIP)
            start_col = 1
            for section_header, section_entries in config.data_sections.items():
                section_width = len(section_entries)
                end_col = start_col + section_width - 1
                if end_col - start_col > 0:
                    ws.merge_range(
                        row,
                        start_col,
                        row,
                        end_col,
                        section_header,
                        styles.DIRECTIVE_STYLE,
                    )
                else:
                    ws.write(row, start_col, section_header, styles.DIRECTIVE_STYLE)
                start_col = end_col + 1

            # DATA SECTION TYPE ANNOTATIONS
            row += 1
            self._write_type_annotation(ws, row, RowType.HEADER)
            annotations = [RowType.DATA.value] * self.DATA_ROWS
            ws.write_column(row + 1, 0, annotations)

            # DATA SECTION ROWS
            for section_entries in config.data_sections.values():
                for entry in section_entries:
                    ws.write(row, col, entry.name, styles.HEADER_STYLE)
                    self._write_comment(ws, row, col, entry, styles)

                    # Write validation to data cells below header cell
                    data_range = xl_range(row + 1, col, row + self.DATA_ROWS, col)
                    self._write_validation(ws, data_range, entry, enum_ranges)
                    col += 1

    def _write_type_annotation(
        self, ws: xlsxwriter.workbook.Worksheet, row: int, row_type: RowType
    ):
        ws.write(row, 0, row_type.value)

    def _write_comment(
        self,
        ws: xlsxwriter.workbook.Worksheet,
        row: int,
        col: int,
        entry: Entry,
        styles: ExcelStyles,
    ):
        comment = entry.doc

        if entry.gcs_uri_format is not None:
            if isinstance(entry.gcs_uri_format, str):
                comment += f'\nIn .{entry.gcs_uri_format.split(".")[-1]} format'
            elif isinstance(entry.gcs_uri_format, dict):
                if "template_comment" in entry.gcs_uri_format:
                    comment += "\n" + entry.gcs_uri_format["template_comment"]

        if comment:
            ws.write_comment(row, col, comment, styles.COMMENT_STYLE_PROPS)

    def _write_validation(
        self,
        ws: xlsxwriter.workbook.Worksheet,
        cell_range: str,
        entry: Entry,
        enum_ranges: Dict[str, str],
    ):
        validation = self._get_validation(cell_range, entry, enum_ranges)
        if validation:
            ws.data_validation(cell_range, validation)

    def _get_validation(
        self, cell_range: str, entry: Entry, data_dict_validations: dict
    ) -> Optional[dict]:
        if entry.enums and len(entry.enums) > 0:
            data_dict_validation_range = data_dict_validations[entry.name]
            return {"validate": "list", "source": data_dict_validation_range}

        elif entry.pytype == datetime.date:
            return {
                "validate": "custom",
                "value": self._make_date_validation_string(cell_range),
                "error_message": "Please enter date in format mm/dd/yyyy",
            }
        elif entry.pytype == datetime.time:
            return {
                "validate": "time",
                "criteria": "between",
                "minimum": datetime.time(0, 0),
                "maximum": datetime.time(23, 59),
                "error_message": "Please enter time in format hh:mm",
            }
        elif entry.pytype == bool:
            return {"validate": "list", "source": ["True", "False"]}

        return None

    @staticmethod
    def _make_date_validation_string(cell_range: str) -> str:
        return f'=AND(ISNUMBER({cell_range}),LEFT(CELL("format",{cell_range}),1)="D")'
