import tempfile
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape


STYLE_DEFAULT = 0
STYLE_HEADER = 1
STYLE_TEXT = 2


def column_name(index: int) -> str:
    result = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def build_columns_xml(widths: list[float]) -> str:
    cols = []
    for index, width in enumerate(widths, start=1):
        cols.append(f'<col min="{index}" max="{index}" width="{width:.2f}" customWidth="1"/>')
    return f"<cols>{''.join(cols)}</cols>"


def build_sheet_xml(rows: list[list[dict]], widths: list[float]) -> str:
    xml_rows = []
    for row_index, cells_data in enumerate(rows, start=1):
        cells = []
        for col_index, cell_data in enumerate(cells_data, start=1):
            cell_ref = f"{column_name(col_index)}{row_index}"
            value = "" if cell_data.get("value") is None else str(cell_data.get("value"))
            style = cell_data.get("style", STYLE_DEFAULT)
            cells.append(
                f'<c r="{cell_ref}" s="{style}" t="inlineStr"><is><t>{escape(value)}</t></is></c>'
            )
        xml_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<sheetPr><outlinePr summaryBelow="1" summaryRight="1"/></sheetPr>'
        f'<dimension ref="A1:{column_name(len(widths))}{max(len(rows), 1)}"/>'
        '<sheetViews><sheetView workbookViewId="0" tabSelected="1"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="18"/>'
        f'{build_columns_xml(widths)}'
        f'<sheetData>{"".join(xml_rows)}</sheetData>'
        f'<autoFilter ref="A1:{column_name(len(widths))}{len(rows)}"/>'
        "</worksheet>"
    )


def estimate_widths(headers: list[str], rows: list[list[str]]) -> list[float]:
    widths = []
    for col_index, header in enumerate(headers):
        max_len = len(str(header))
        for row in rows:
            if col_index < len(row):
                max_len = max(max_len, len(str(row[col_index])))
        widths.append(min(max(max_len + 2, 12), 36))
    return widths


def build_workbook_bytes(sheet_name: str, headers: list[str], rows: list[list[str]]) -> bytes:
    sheet_rows = [
        [{"value": header, "style": STYLE_HEADER} for header in headers],
        *[[{"value": value, "style": STYLE_TEXT} for value in row] for row in rows],
    ]
    widths = estimate_widths(headers, rows)

    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView activeTab="0"/></bookViews>'
        '<sheets>'
        f'<sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/>'
        "</sheets></workbook>"
    )

    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        "</Relationships>"
    )

    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        "</Relationships>"
    )

    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="3">'
        '<font><sz val="11"/><name val="Calibri"/></font>'
        '<font><b/><sz val="12"/><name val="Calibri"/></font>'
        '<font><sz val="11"/><name val="Calibri"/></font>'
        '</fonts>'
        '<fills count="4">'
        '<fill><patternFill patternType="none"/></fill>'
        '<fill><patternFill patternType="gray125"/></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="FFF4E7D1"/><bgColor indexed="64"/></patternFill></fill>'
        '<fill><patternFill patternType="solid"><fgColor rgb="FFFBF6EE"/><bgColor indexed="64"/></patternFill></fill>'
        '</fills>'
        '<borders count="2">'
        '<border/>'
        '<border><left style="thin"><color rgb="FFD8CEC0"/></left><right style="thin"><color rgb="FFD8CEC0"/></right><top style="thin"><color rgb="FFD8CEC0"/></top><bottom style="thin"><color rgb="FFD8CEC0"/></bottom></border>'
        '</borders>'
        '<cellStyleXfs count="1"><xf/></cellStyleXfs>'
        '<cellXfs count="3">'
        '<xf xfId="0" fontId="0" fillId="0" borderId="0"/>'
        '<xf xfId="0" fontId="1" fillId="2" borderId="1" applyFont="1" applyFill="1" applyBorder="1"/>'
        '<xf xfId="0" fontId="2" fillId="3" borderId="1" applyFont="1" applyFill="1" applyBorder="1"/>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )

    with tempfile.NamedTemporaryFile(mode="wb", suffix=".xlsx", delete=False) as fh:
        tmp_path = Path(fh.name)
        with zipfile.ZipFile(fh, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("[Content_Types].xml", content_types)
            zf.writestr("_rels/.rels", root_rels)
            zf.writestr("xl/workbook.xml", workbook_xml)
            zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
            zf.writestr("xl/styles.xml", styles_xml)
            zf.writestr("xl/worksheets/sheet1.xml", build_sheet_xml(sheet_rows, widths))
        payload = tmp_path.read_bytes()
    try:
        tmp_path.unlink()
    except Exception:
        pass
    return payload
