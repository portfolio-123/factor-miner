import math

import streamlit as st
import streamlit.components.v1 as components
import polars as pl

from src.core.utils.common import escape_html, escape_html_attr

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
    .html-table--sortable th {
        cursor: pointer;
        user-select: none;
    }
    .html-table--sortable th:hover {
        background: #e4e6ea;
    }
    .html-table--sortable th .sort-indicator {
        margin-left: 4px;
        opacity: 0.3;
    }
    .html-table--sortable th.sort-asc .sort-indicator,
    .html-table--sortable th.sort-desc .sort-indicator {
        opacity: 1;
    }
</style>
"""

SORT_SCRIPT = """
<script>
(function() {
    const tableId = '__TABLE_ID__';
    const table = document.getElementById(tableId);
    if (!table) return;

    const headers = table.querySelectorAll('th');
    let currentSort = { col: -1, asc: true };

    headers.forEach((th, colIdx) => {
        th.addEventListener('click', () => sortTable(colIdx));
    });

    function sortTable(colIdx) {
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));

        const asc = currentSort.col === colIdx ? !currentSort.asc : true;
        currentSort = { col: colIdx, asc };

        rows.sort((a, b) => {
            const aCell = a.children[colIdx];
            const bCell = b.children[colIdx];
            const aVal = aCell.dataset.sortValue !== undefined ? aCell.dataset.sortValue : aCell.textContent.trim();
            const bVal = bCell.dataset.sortValue !== undefined ? bCell.dataset.sortValue : bCell.textContent.trim();

            const aNum = parseFloat(aVal);
            const bNum = parseFloat(bVal);

            let cmp;
            if (!isNaN(aNum) && !isNaN(bNum)) {
                cmp = aNum - bNum;
            } else {
                cmp = aVal.localeCompare(bVal);
            }
            return asc ? cmp : -cmp;
        });

        rows.forEach(row => tbody.appendChild(row));

        headers.forEach((th, idx) => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (idx === colIdx) {
                th.classList.add(asc ? 'sort-asc' : 'sort-desc');
                th.querySelector('.sort-indicator').textContent = asc ? '▲' : '▼';
            } else {
                th.querySelector('.sort-indicator').textContent = '▲';
            }
        });
    }
})();
</script>
"""


def render_table(
    df: pl.DataFrame,
    *,
    row_colors: list[str] | None = None,
    row_links: list[str] | None = None,
    format_spec: dict[str, str] | None = None,
    column_widths: dict[str, str] | None = None,
    max_height=400,
    zebra=False,
    small_headers=False,
    sortable=False,
) -> None:
    headers = df.columns
    is_clickable = row_links is not None

    if row_colors is None and zebra:
        row_colors = ["#ffffff" if i % 2 == 0 else "#f8f8f8" for i in range(len(df))]

    table_classes = ["html-table"]
    if is_clickable:
        table_classes.append("html-table--clickable")
    if sortable:
        table_classes.append("html-table--sortable")
    table_class = " ".join(table_classes)

    table_id = f"table-{id(df)}"

    html = TABLE_STYLES
    html += f'<div class="html-table-container" style="max-height: {max_height}px;">'
    html += f'<table class="{table_class}" id="{table_id}">'

    html += "<thead><tr>"
    for col in headers:
        styles = []
        if small_headers:
            styles.append("font-size: 11px")
        if column_widths and col in column_widths:
            styles.append(f"width: {column_widths[col]}")
        style_attr = f' style="{"; ".join(styles)}"' if styles else ""
        sort_indicator = '<span class="sort-indicator">▲</span>' if sortable else ""
        html += f"<th{style_attr}>{escape_html(str(col))}{sort_indicator}</th>"
    html += "</tr></thead>"

    html += "<tbody>"
    for i, row in enumerate(df.iter_rows(named=True)):
        bg_color = row_colors[i] if row_colors and i < len(row_colors) else "#ffffff"
        link = row_links[i] if row_links and i < len(row_links) else None

        html += f'<tr style="background: {bg_color}" data-original-bg="{bg_color}">'
        for j, col in enumerate(headers):
            value = row[col]

            if value is None:
                cell_value = ""
                sort_value = ""
            elif isinstance(value, float) and math.isnan(value):
                cell_value = "-"
                sort_value = ""
            elif format_spec and col in format_spec:
                fmt = format_spec[col]
                sort_value = str(value) if isinstance(value, (int, float)) else ""
                if fmt.endswith("%"):
                    cell_value = escape_html(f"{value:{fmt[:-1]}}%")
                else:
                    cell_value = escape_html(f"{value:{fmt}}")
            elif isinstance(value, (int, float)):
                cell_value = value
                sort_value = str(value)
            else:
                cell_value = escape_html(value)
                sort_value = ""

            td_styles = []
            if j == 0 and small_headers:
                td_styles.extend(["font-size: 10px", "font-weight: 600"])
            if column_widths and col in column_widths and column_widths[col].endswith("px"):
                td_styles.append("white-space: nowrap")
            td_style_attr = f' style="{"; ".join(td_styles)}"' if td_styles else ""

            sort_attr = f' data-sort-value="{sort_value}"' if sortable and sort_value else ""

            cell_content = f'<a href="{escape_html_attr(link)}">{cell_value}</a>' if link else cell_value
            html += f"<td{td_style_attr}{sort_attr}>{cell_content}</td>"

        html += "</tr>"

    html += "</tbody></table></div>"

    if sortable:
        html += SORT_SCRIPT.replace("__TABLE_ID__", table_id)
        components.html(html, height=max_height + 50, scrolling=True)
    else:
        st.html(html)
