from enum import Enum
from datetime import date, time
from typing import Optional, Dict, Callable, Any, List

import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
from sqlalchemy import Column, Enum as SqlEnum


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
        process_as: Optional[Dict[Column, Callable[[str], Any]]] = None,
        encrypt: bool = False,
    ):
        self.column = column
        self.name = name if name else column.name.replace("_", " ").upper()
        self.gcs_uri_format = gcs_uri_format
        self.process_as = process_as
        self.encrypt = encrypt

        self.doc = column.doc
        self.sqltype = column.type
        self.enums = self.sqltype.enums if isinstance(self.sqltype, SqlEnum) else None
        self.pytype = self.sqltype.python_type


class WorksheetConfig:
    """
    A worksheet within a metadata spreadsheet. A worksheet has a name, a list
    of `preamble_rows` and a dictionary mapping data column group headers to lists
    of `data_columns`.
    etc. etc...
    """

    def __init__(
        self,
        name: str,
        preamble_rows: List[Entry] = [],
        data_columns: Dict[str, List[Entry]] = {},
    ):
        self.name = name
        self.preamble_rows = preamble_rows
        self.data_columns = data_columns


class RowType(Enum):
    """Annotations denoting what type of data a template row contains."""

    TITLE = "#title"
    SKIP = "#skip"
    HEADER = "#header"
    PREAMBLE = "#preamble"
    DATA = "#data"


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
    """Data class containing format specifications used in `XlTemplateWriter`"""

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

    @classmethod
    def read(cls, filename: str):
        """
        Extract a list of SQLAlchemy models in insertion order from a populated
        instance of this template.
        """
        ...

    @classmethod
    def write(cls, filename: str):
        """
        Generate an empty excel file for this template type.
        """
        workbook = xlsxwriter.Workbook(filename)
        styles = ExcelStyles(workbook)
        cls._write_legend(workbook, styles)
        enum_ranges = cls._write_data_dict(workbook, styles)
        cls._write_worksheets(workbook, styles, enum_ranges)
        workbook.close()

    @classmethod
    def _write_legend(cls, workbook: xlsxwriter.Workbook, styles: ExcelStyles):
        ws = workbook.add_worksheet("Legend")
        ws.protect()
        ws.set_column(1, 100, width=styles.COLUMN_WIDTH)

        row = 0
        ws.write(row, 1, f"LEGEND", styles.DATA_STYLE)

        for config in cls.worksheet_configs:
            row += 1
            ws.write(row, 1, f"Legend for tab {config.name!r}", styles.TITLE_STYLE)

            for entry in config.preamble_rows:
                row += 1
                cls._write_legend_item(ws, row, entry, styles.PREAMBLE_STYLE)

            for section_name, section_entries in config.data_columns.items():
                row += 1
                ws.write(
                    row,
                    1,
                    f"Section {section_name!r} of tab {config.name!r}",
                    styles.DIRECTIVE_STYLE,
                )

                for entry in section_entries:
                    row += 1
                    cls._write_legend_item(ws, row, entry, styles.HEADER_STYLE)

    @classmethod
    def _write_legend_item(
        cls,
        ws: xlsxwriter.workbook.Worksheet,
        row: int,
        entry: Entry,
        style: xlsxwriter.workbook.Format,
    ):
        """ Writes a property with its type, description, and example if any."""
        ws.write(row, 1, entry.name.upper(), style)
        ws.write(row, 2, entry.sqltype.__class__.__name__)
        ws.write(row, 3, entry.doc)

    @classmethod
    def _write_data_dict(
        cls, workbook: xlsxwriter.Workbook, styles: ExcelStyles
    ) -> Dict[str, str]:
        ws = workbook.add_worksheet(cls.DATA_DICT_SHEETNAME)
        ws.protect()
        ws.set_column(1, 100, width=styles.COLUMN_WIDTH)

        col = 0

        # a result dictionary that maps field names to data dictionary sheet
        # ranges of enum values to be used for validation
        data_dict_mapping = {}

        for config in cls.worksheet_configs:
            for entry in config.preamble_rows:
                rows = cls._write_data_dict_item(ws, col, entry, styles.PREAMBLE_STYLE)
                if rows > 0:
                    # saving Data Dict range to use for validation
                    data_dict_mapping[entry.name] = _format_validation_range(
                        rows, col, cls.DATA_DICT_SHEETNAME
                    )
                    col += 1

            for section_entries in config.data_columns.values():
                for entry in section_entries:
                    rows = cls._write_data_dict_item(
                        ws, col, entry, styles.HEADER_STYLE
                    )
                    if rows > 0:
                        # saving Data Dict range to use for validation
                        data_dict_mapping[entry.name] = _format_validation_range(
                            rows, col, cls.DATA_DICT_SHEETNAME
                        )
                        col += 1

        return data_dict_mapping

    @classmethod
    def _write_data_dict_item(
        cls,
        ws: xlsxwriter.workbook.Worksheet,
        col: int,
        entry: Entry,
        style: xlsxwriter.workbook.Format,
    ):
        if entry.enums is None:
            return 0

        # Write the data dict column header
        ws.write(0, col, entry.name.upper(), style)

        # Write the data dict column values
        for i, enum_value in enumerate(entry.enums):
            ws.write(1 + i, col, enum_value)

        return len(entry.enums)

    @classmethod
    def _write_worksheets(
        cls,
        workbook: xlsxwriter.Workbook,
        styles: ExcelStyles,
        enum_ranges: Dict[str, str],
    ):
        """Write content to the given worksheet"""
        for config in cls.worksheet_configs:
            ws = workbook.add_worksheet(config.name)
            ws.set_column(0, 100, width=styles.COLUMN_WIDTH)

            row = 0
            col = 1

            # WORKSHEET TITLE
            cls._write_type_annotation(ws, row, RowType.TITLE)
            preamble_range = xl_range(row, 1, row, 2)
            ws.merge_range(preamble_range, config.name.upper(), styles.TITLE_STYLE)

            # PREAMBLE ROWS
            row += 1
            for entry in config.preamble_rows:
                # Write row type and entity name
                cls._write_type_annotation(ws, row, RowType.PREAMBLE)
                ws.write(row, 1, entry.name.upper(), styles.PREAMBLE_STYLE)
                cls._write_comment(ws, row, 1, entry, styles)

                # Format value cells next to entity name
                ws.write(row, 2, "", styles.PREAMBLE_STYLE)

                # Add data validation if appropriate
                value_cell = xl_rowcol_to_cell(row, 2)
                cls._write_validation(ws, value_cell, entry, enum_ranges)
                row += 1

            # DATA SECTION HEADERS
            row += 1
            cls._write_type_annotation(ws, row, RowType.SKIP)
            start_col = 1
            for section_header, section_entries in config.data_columns.items():
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
            cls._write_type_annotation(ws, row, RowType.HEADER)
            annotations = [RowType.DATA.value] * cls.DATA_ROWS
            ws.write_column(row + 1, 0, annotations)

            # DATA SECTION ROWS
            for section_entries in config.data_columns.values():
                for entry in section_entries:
                    ws.write(row, col, entry.name.upper(), styles.HEADER_STYLE)
                    cls._write_comment(ws, row, col, entry, styles)

                    # Write validation to data cells below header cell
                    data_range = xl_range(row + 1, col, row + cls.DATA_ROWS, col)
                    cls._write_validation(ws, data_range, entry, enum_ranges)
                    col += 1

    @classmethod
    def _write_type_annotation(
        cls, ws: xlsxwriter.workbook.Worksheet, row: int, row_type: RowType
    ):
        ws.write(row, 0, row_type.value)

    @classmethod
    def _write_comment(
        cls,
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
            ws.write_comment(row, col, comment, styles.COMMENT_STYLE)

    @classmethod
    def _write_validation(
        cls,
        ws: xlsxwriter.workbook.Worksheet,
        cell_range: str,
        entry: Entry,
        enum_ranges: Dict[str, str],
    ):
        validation = cls._get_validation(cell_range, entry, enum_ranges)
        if validation:
            ws.data_validation(cell_range, validation)

    @classmethod
    def _get_validation(
        cls, cell_range: str, entry: Entry, data_dict_validations: dict
    ) -> Optional[dict]:
        if entry.enums and len(entry.enums) > 0:
            data_dict_validation_range = data_dict_validations[entry.name]
            return {"validate": "list", "source": data_dict_validation_range}

        elif entry.pytype == date:
            return {
                "validate": "custom",
                "value": cls._make_date_validation_string(cell_range),
                "error_message": "Please enter date in format mm/dd/yyyy",
            }
        elif entry.pytype == time:
            return {
                "validate": "time",
                "criteria": "between",
                "minimum": time(0, 0),
                "maximum": time(23, 59),
                "error_message": "Please enter time in format hh:mm",
            }
        elif entry.pytype == bool:
            return {"validate": "list", "source": ["True", "False"]}

        return None

    @staticmethod
    def _make_date_validation_string(cell_range: str) -> str:
        return f'=AND(ISNUMBER({cell_range}),LEFT(CELL("format",{cell_range}),1)="D")'


### Template example ###

from cidc_api.models.models import Permissions, TrialMetadata, UploadJobs


class PBMCTemplate(MetadataTemplate):
    upload_type = "pbmc"
    worksheet_configs = [
        WorksheetConfig(
            "sheet1",
            [Entry(TrialMetadata.trial_id)],
            {
                "section1": [Entry(UploadJobs.status, name="Upload Status")],
                "section2": [Entry(Permissions.upload_type)],
            },
        ),
        WorksheetConfig(
            "sheet2",
            [Entry(TrialMetadata.trial_id)],
            {
                "section1": [Entry(UploadJobs.status, name="Upload Status")],
                "section2": [Entry(Permissions.upload_type)],
            },
        ),
    ]


if __name__ == "__main__":
    PBMCTemplate.write("test.xlsx")
