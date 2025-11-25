def get_app_css() -> str:
    return """
<style>

.horizontal-radio .widget-radio-box {
    display: flex !important;
    flex-direction: row !important;
    align-items: center !important;
    overflow: visible !important;
    height: auto !important;
    padding: 0 !important;
}
.widget-box {
    overflow: visible !important;
}

.horizontal-radio label {
    margin-right: 20px !important;
    margin-top: 0 !important;
    margin-bottom: 0 !important;
    padding: 0 !important;
    display: flex !important;
    align-items: center !important;
    flex-direction: row !important;
    height: auto !important;
}
.horizontal-radio label input[type="radio"] {
    margin-right: 5px !important;
    margin-left: 0 !important;
    margin-top: 0 !important;
    margin-bottom: 0 !important;
    order: -1 !important;
}
.horizontal-radio {
    padding: 0 !important;
    overflow: visible !important;
    min-height: 0 !important;
}

.section-header {
    font-size: 14px;
    font-weight: 600;
    color: #2196F3;
    margin: 10px 0 8px 0;
    padding-bottom: 5px;
    border-bottom: 2px solid #2196F3;
}

.brand-title {
    font-size: 24px;
    font-weight: 700;
    color: #333;
    margin: 0;
    line-height: 1.2;
}

.brand-subtitle {
    font-size: 16px;
    font-weight: 400;
    color: #666;
    margin: 2px 0 0 0;
}

.data-preview {
    background-color: #f5f5f5;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 15px;
    margin: 10px 0;
    font-family: monospace;
    font-size: 12px;
    max-height: 400px;
    overflow: auto;
}

.data-preview h4 {
    margin-top: 0;
    color: #333;
    font-family: sans-serif;
}

.data-preview table {
    width: 100%;
    border-collapse: collapse;
    background-color: white;
}

.data-preview th,
.data-preview td {
    padding: 8px;
    text-align: left;
    border: 1px solid #ddd;
}

.data-preview th {
    background-color: #e8e8e8;
    font-weight: bold;
}

.error-box {
    background-color: #ffebee;
    border-left: 4px solid #d32f2f;
    padding: 12px 16px;
    margin: 10px 0;
    border-radius: 4px;
    color: #c62828;
    font-size: 14px;
}
</style>
"""
