from src.internal.links import p123_link


def format_analysis_error(fl_id: str, error: str) -> str:
    msg = error.split("\n")[0]

    if "[column-not-found]" in msg:
        if link := p123_link(fl_id, "factors"):  # external
            gen_link = p123_link(fl_id, "generate")
            return f"{msg}\n\n[Add Missing]({link}) | [Regenerate]({gen_link})"
        return f"{msg}\n\nEnsure your parquet file contains this column."  # internal

    if "[single-date]" in msg:
        if link := p123_link(fl_id, "generate"):  # external
            return f"{msg}\n\nPlease [generate a new dataset]({link}) using Period."
        return f"{msg}\n\nSingle-date datasets are not supported. Use a multi-period dataset."  # internal

    return f"Analysis failed: {msg}"
