import streamlit as st
import polars as pl
from html import escape


TABLE_STYLES = """
<style>
    .html-table-container {
        overflow-y: auto;
        border: 1px solid #ddd;
        border-radius: 4px;
    }
    .html-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }
    .html-table th {
        background: #f0f2f6;
        padding: 8px 6px;
        text-align: left;
        font-weight: 600;
        border-bottom: 1px solid #ddd;
        border-right: 1px solid #ddd;
        white-space: nowrap;
        position: sticky;
        top: 0;
        z-index: 1;
    }
    .html-table th:last-child {
        border-right: none;
    }
    .html-table td {
        padding: 6px;
        border-bottom: 1px solid #ddd;
        border-right: 1px solid #ddd;
        word-break: break-word;
    }
    .html-table td:last-child {
        border-right: none;
    }
    .html-table tbody tr:last-child td {
        border-bottom: none;
    }
    .html-table--clickable td {
        padding: 0;
    }
    .html-table--clickable td a {
        display: block;
        padding: 6px;
        color: inherit;
        text-decoration: none;
    }
    .html-table--clickable tr:hover {
        filter: brightness(0.93);
    }
</style>
"""


def render_table(
    df: pl.DataFrame,
    *,
    row_colors: list[str] | None = None,
    row_links: list[str] | None = None,
    format_spec: dict[str, str] | None = None,
    column_widths: dict[str, str] | None = None,
    max_height: int = 400,
    zebra: bool = False,
    small_headers: bool = False,
) -> None:
    headers = df.columns
    is_clickable = row_links is not None

    if row_colors is None and zebra:
        row_colors = ["#ffffff" if i % 2 == 0 else "#f8f8f8" for i in range(len(df))]

    table_class = "html-table html-table--clickable" if is_clickable else "html-table"

    html = TABLE_STYLES
    html += f'<div class="html-table-container" style="max-height: {max_height}px;">'
    html += f'<table class="{table_class}">'

    html += "<thead><tr>"
    for col in headers:
        styles = []
        if small_headers:
            styles.append("font-size: 11px")
        if column_widths and col in column_widths:
            styles.append(f"width: {column_widths[col]}")
        style_attr = f' style="{"; ".join(styles)}"' if styles else ""
        html += f"<th{style_attr}>{escape(str(col))}</th>"
    html += "</tr></thead>"

    html += "<tbody>"
    for i, row in enumerate(df.iter_rows(named=True)):
        bg_color = row_colors[i] if row_colors and i < len(row_colors) else "#ffffff"
        link = row_links[i] if row_links and i < len(row_links) else None

        html += f'<tr style="background: {bg_color}">'
        for j, col in enumerate(headers):
            value = row[col]
            if value is None:
                cell_value = "N/A"
            elif format_spec and col in format_spec:
                fmt = format_spec[col]
                if fmt.endswith("%"):
                    cell_value = escape(f"{value:{fmt[:-1]}}%")
                else:
                    cell_value = escape(f"{value:{fmt}}")
            else:
                cell_value = escape(str(value))

            first_col_style = ' style="font-size: 10px; font-weight: 600;"' if j == 0 and small_headers else ""

            if link:
                html += f'<td{first_col_style}><a href="{link}" target="_top">{cell_value}</a></td>'
            else:
                html += f"<td{first_col_style}>{cell_value}</td>"

        html += "</tr>"

    html += "</tbody></table></div>"

    st.html(html)
